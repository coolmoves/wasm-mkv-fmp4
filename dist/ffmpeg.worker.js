/**
 * ffmpeg.worker.js
 * Streaming MKV → fMP4 (fragmented MP4) converter running in a Web Worker.
 * Zero browser API dependencies — works in Worker, Node.js worker_threads, Deno.
 *
 * Protocol (postMessage / onmessage):
 *
 *   HOST → WORKER
 *   ──────────────
 *   { type: 'init', wasmURL, coreURL }          -- required first message
 *   { type: 'start', inputChunks?: Uint8Array[] }-- begin a conversion job
 *   { type: 'chunk', data: Uint8Array }          -- feed input bytes
 *   { type: 'end' }                              -- signal end-of-input
 *   { type: 'abort' }                            -- cancel current job
 *
 *   WORKER → HOST
 *   ──────────────
 *   { type: 'ready' }                            -- WASM loaded, ready for jobs
 *   { type: 'output', data: Uint8Array }         -- fMP4 output chunk (Transferable)
 *   { type: 'progress', ratio: 0-1, time: secs }
 *   { type: 'log', level: 'info'|'error', msg }
 *   { type: 'done' }                             -- conversion finished
 *   { type: 'error', message }                   -- fatal error
 */

'use strict';

/* ─────────────────────────────────────────────────────────────────────────────
   State
───────────────────────────────────────────────────────────────────────────── */

let Module = null;          // loaded WASM module
let coreReady = false;
let jobActive = false;
let abortRequested = false;

// Queued input chunks before we call ffmpeg
const inputQueue = [];
let inputEnded = false;

/* ─────────────────────────────────────────────────────────────────────────────
   Message dispatch
───────────────────────────────────────────────────────────────────────────── */

self.onmessage = async (evt) => {
  const msg = evt.data;
  switch (msg.type) {
    case 'init':    return handleInit(msg);
    case 'start':   return handleStart(msg);
    case 'chunk':   return handleChunk(msg);
    case 'end':     return handleEnd();
    case 'abort':   return handleAbort();
    default:
      post({ type: 'error', message: `Unknown message type: ${msg.type}` });
  }
};

/* ─────────────────────────────────────────────────────────────────────────────
   init — load WASM
───────────────────────────────────────────────────────────────────────────── */

async function handleInit({ wasmURL, coreURL }) {
  try {
    // Load the ESM core script.  We need dynamic import or importScripts.
    // importScripts works in dedicated workers; fallback to fetch+eval for
    // module workers / Node.
    const mod = await import(
     coreURL || new URL('./ffmpeg-core.js', import.meta.url).href
   );
   
   const createFFmpegCore =
     mod.default || mod.createFFmpegCore;

    Module = await createFFmpegCore({
      // Provide WASM bytes directly so no fetch() is needed
      wasmBinary: await fetchBytes(wasmURL || new URL('ffmpeg-core.wasm', getBaseURL(coreURL)).href),
      // Suppress the "Aborted" stderr spam
      printErr: (msg) => {
        if (!msg.startsWith('Aborted')) post({ type: 'log', level: 'error', msg });
      },
      print: (msg) => post({ type: 'log', level: 'info', msg }),
    });

    coreReady = true;
    post({ type: 'ready' });
  } catch (err) {
    post({ type: 'error', message: `Init failed: ${err.message}` });
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   start — kick off a conversion job
───────────────────────────────────────────────────────────────────────────── */

async function handleStart({ inputChunks } = {}) {
  if (!coreReady) {
    post({ type: 'error', message: 'Worker not initialized. Send { type:"init" } first.' });
    return;
  }
  if (jobActive) {
    post({ type: 'error', message: 'A conversion is already in progress.' });
    return;
  }

  jobActive = true;
  abortRequested = false;
  inputQueue.length = 0;
  inputEnded = false;

  if (inputChunks && inputChunks.length) {
    inputQueue.push(...inputChunks.map(c => toUint8Array(c)));
    inputEnded = true;        // all data already provided
  }

  runConversion();
}

/* ─────────────────────────────────────────────────────────────────────────────
   chunk / end / abort
───────────────────────────────────────────────────────────────────────────── */

function handleChunk({ data }) {
  if (!jobActive) {
    post({ type: 'error', message: 'No active job. Send { type:"start" } first.' });
    return;
  }
  inputQueue.push(toUint8Array(data));
}

function handleEnd() {
  inputEnded = true;
}

function handleAbort() {
  abortRequested = true;
}

/* ─────────────────────────────────────────────────────────────────────────────
   Core conversion logic
───────────────────────────────────────────────────────────────────────────── */

async function runConversion() {
  const FS = Module.FS;
  const INPUT_PATH  = '/work/input.mkv';
  const OUTPUT_PATH = '/work/output.mp4';

  try {
    // 1. Set up virtual filesystem
    FS.mkdir('/work');

    // 2. Collect all input into MEMFS
    //    For streaming input we poll the queue until inputEnded + queue empty
    post({ type: 'log', level: 'info', msg: '[ffmpeg-worker] Buffering input…' });

    const inputChunks = await collectInput();
    if (abortRequested) throw new Error('aborted');

    const inputData = mergeChunks(inputChunks);
    post({ type: 'log', level: 'info', msg: `[ffmpeg-worker] Input size: ${inputData.byteLength} bytes` });

    // Write input to MEMFS
    FS.writeFile(INPUT_PATH, inputData);

    // 3. Wire up progress
    Module.setProgress(({ ratio, time }) => {
      post({ type: 'progress', ratio, time });
    });

    // 4. Run FFmpeg: MKV → fragmented MP4
    //    -movflags frag_keyframe+empty_moov+default_base_moof produces fMP4
    //    -c copy avoids re-encoding (fastest; preserves H.264/AAC as-is)
    //    Fall back to -c:v libx264 -c:a aac if the source codec isn't passthrough-safe
    const args = [
      '-i',  INPUT_PATH,
      '-c',  'copy',                          // stream copy — no re-encode
      '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
      '-reset_timestamps', '1',
      '-f',  'mp4',
      OUTPUT_PATH,
    ];

    post({ type: 'log', level: 'info', msg: `[ffmpeg-worker] Running: ffmpeg ${args.join(' ')}` });

    Module.setLogger(({ type, message }) => {
      post({ type: 'log', level: type === 'stderr' ? 'error' : 'info', msg: message });
    });

    const ret = Module.exec(...args);

    if (abortRequested) throw new Error('aborted');
    if (ret !== 0) throw new Error(`FFmpeg exited with code ${ret}`);

    // 5. Read output and stream it out in chunks
    const output = FS.readFile(OUTPUT_PATH);
    post({ type: 'log', level: 'info', msg: `[ffmpeg-worker] Output size: ${output.byteLength} bytes` });

    const CHUNK = 256 * 1024; // 256 KB chunks
    for (let offset = 0; offset < output.byteLength; offset += CHUNK) {
      const slice = output.slice(offset, offset + CHUNK);
      self.postMessage({ type: 'output', data: slice }, [slice.buffer]);
    }

    post({ type: 'done' });

  } catch (err) {
    post({ type: 'error', message: err.message });
  } finally {
    // Cleanup MEMFS
    try { Module.FS.unlink('/work/input.mkv');  } catch (_) {}
    try { Module.FS.unlink('/work/output.mp4'); } catch (_) {}
    try { Module.FS.rmdir('/work');             } catch (_) {}
    Module.reset();
    jobActive = false;
  }
}

/* ─────────────────────────────────────────────────────────────────────────────
   Helpers
───────────────────────────────────────────────────────────────────────────── */

/** Poll inputQueue until inputEnded and queue is drained */
async function collectInput() {
  const chunks = [];
  while (true) {
    while (inputQueue.length) chunks.push(inputQueue.shift());
    if (inputEnded) break;
    if (abortRequested) break;
    await sleep(10);
  }
  while (inputQueue.length) chunks.push(inputQueue.shift());
  return chunks;
}

function mergeChunks(chunks) {
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) { out.set(c, offset); offset += c.byteLength; }
  return out;
}

function toUint8Array(data) {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  return new Uint8Array(data);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function post(msg) {
  self.postMessage(msg);
}

function getBaseURL(coreURL) {
  if (coreURL) {
    return coreURL.substring(0, coreURL.lastIndexOf('/') + 1);
  }
  return self.location ? self.location.href.substring(0, self.location.href.lastIndexOf('/') + 1) : './';
}

async function fetchBytes(url) {
  // Works in workers (no window) via fetch()
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to fetch ${url}: ${resp.status}`);
  return new Uint8Array(await resp.arrayBuffer());
}
