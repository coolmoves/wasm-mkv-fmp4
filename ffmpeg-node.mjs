/**
 * ffmpeg-node.mjs
 * Node.js adapter for the FFmpeg WASM streaming converter.
 * Uses worker_threads instead of Web Workers — no browser APIs needed.
 *
 * Usage:
 *   import { convertMkvToFmp4 } from './ffmpeg-node.mjs';
 *   import { createReadStream } from 'fs';
 *
 *   // ── Option A: file → Buffer ──────────────────────────────────────────────
 *   const fmp4Buffer = await convertMkvToFmp4('./input.mkv');
 *
 *   // ── Option B: stream → stream ────────────────────────────────────────────
 *   const readable = createReadStream('./input.mkv');
 *   const { outputStream } = await convertMkvToFmp4Stream(readable);
 *   outputStream.pipe(createWriteStream('./output.mp4'));
 */

import { createRequire } from 'module';
import { fileURLToPath, pathToFileURL } from 'url';
import { dirname, join } from 'path';
import { readFile } from 'fs/promises';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { Readable, Transform, PassThrough } from 'stream';

const __dirname = dirname(fileURLToPath(import.meta.url));
const WASM_PATH = join(__dirname, 'ffmpeg-core.wasm');
const CORE_PATH = join(__dirname, 'ffmpeg-core.js');

/* ─────────────────────────────────────────────────────────────────────────────
   Worker thread entrypoint (same file, dual-mode)
───────────────────────────────────────────────────────────────────────────── */

if (!isMainThread) {
  // We're running inside the worker thread
  runWorkerThread();
}

async function runWorkerThread() {
  // Polyfill self for the WASM module (expects worker-like global)
  global.self = global;
  global.performance = global.performance || { now: () => Date.now() };

  // Load wasm bytes ahead of time to avoid fetch()
  const wasmBinary = await readFile(WASM_PATH);

  // Load the UMD build which doesn't use import.meta
  const { createRequire: cr } = await import('module');
  const require = cr(pathToFileURL(CORE_PATH).href);

  // The UMD file sets global.createFFmpegCore
  const src = await readFile(CORE_PATH, 'utf8');
  const umdWrapped = `(function(self){${src}})(global)`;
  /* eslint-disable no-eval */
  eval(umdWrapped);

  const Module = await global.createFFmpegCore({
    wasmBinary,
    print:    (msg) => parentPort.postMessage({ type: 'log', level: 'info',  msg }),
    printErr: (msg) => {
      if (!msg.startsWith('Aborted')) {
        parentPort.postMessage({ type: 'log', level: 'error', msg });
      }
    },
  });

  Module.setProgress(({ ratio, time }) => {
    parentPort.postMessage({ type: 'progress', ratio, time });
  });

  parentPort.postMessage({ type: 'ready' });

  parentPort.on('message', async (msg) => {
    switch (msg.type) {
      case 'convert': {
        try {
          const inputData = new Uint8Array(msg.input);
          Module.FS.mkdir('/work');
          Module.FS.writeFile('/work/input.mkv', inputData);

          const args = [
            '-i',  '/work/input.mkv',
            '-c',  'copy',
            '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
            '-reset_timestamps', '1',
            '-f',  'mp4',
            '/work/output.mp4',
          ];

          const ret = Module.exec(...args);
          if (ret !== 0) throw new Error(`FFmpeg exited with code ${ret}`);

          const output = Module.FS.readFile('/work/output.mp4');

          // Stream output in chunks
          const CHUNK = 256 * 1024;
          for (let off = 0; off < output.byteLength; off += CHUNK) {
            const slice = output.slice(off, off + CHUNK);
            parentPort.postMessage({ type: 'output', data: slice.buffer }, [slice.buffer]);
          }
          parentPort.postMessage({ type: 'done' });
        } catch (err) {
          parentPort.postMessage({ type: 'error', message: err.message });
        } finally {
          try { Module.FS.unlink('/work/input.mkv');  } catch (_) {}
          try { Module.FS.unlink('/work/output.mp4'); } catch (_) {}
          try { Module.FS.rmdir('/work');             } catch (_) {}
          Module.reset();
        }
        break;
      }
    }
  });
}

/* ─────────────────────────────────────────────────────────────────────────────
   Main thread API
───────────────────────────────────────────────────────────────────────────── */

function createWorker() {
  return new Promise((resolve, reject) => {
    const worker = new Worker(fileURLToPath(import.meta.url));
    worker.once('message', (msg) => {
      if (msg.type === 'ready') resolve(worker);
      else reject(new Error(msg.message));
    });
    worker.once('error', reject);
  });
}

/**
 * Convert an MKV file (path or Buffer) to fMP4.
 * @param {string|Buffer|Uint8Array} input  - file path or raw bytes
 * @param {object} [opts]
 * @param {function} [opts.onProgress]      - ({ratio, time}) => void
 * @param {function} [opts.onLog]           - ({level, msg}) => void
 * @returns {Promise<Buffer>} fragmented MP4 bytes
 */
export async function convertMkvToFmp4(input, opts = {}) {
  const { onProgress, onLog } = opts;
  let inputBytes;
  if (typeof input === 'string') {
    inputBytes = await readFile(input);
  } else {
    inputBytes = input instanceof Buffer ? input : Buffer.from(input);
  }

  const worker = await createWorker();

  return new Promise((resolve, reject) => {
    const chunks = [];

    worker.on('message', (msg) => {
      switch (msg.type) {
        case 'output':
          chunks.push(Buffer.from(msg.data));
          break;
        case 'progress':
          onProgress && onProgress({ ratio: msg.ratio, time: msg.time });
          break;
        case 'log':
          onLog && onLog({ level: msg.level, msg: msg.msg });
          break;
        case 'done':
          worker.terminate();
          resolve(Buffer.concat(chunks));
          break;
        case 'error':
          worker.terminate();
          reject(new Error(msg.message));
          break;
      }
    });

    worker.postMessage({ type: 'convert', input: inputBytes.buffer }, [inputBytes.buffer]);
  });
}

/**
 * Convert a Readable stream of MKV data to a Readable stream of fMP4.
 * @param {Readable} inputStream
 * @param {object}  [opts]
 * @returns {Promise<{ outputStream: Readable, worker: Worker }>}
 */
export async function convertMkvToFmp4Stream(inputStream, opts = {}) {
  const chunks = [];
  for await (const chunk of inputStream) chunks.push(chunk);
  const inputBytes = Buffer.concat(chunks);

  const outputPass = new PassThrough();
  const worker = await createWorker();

  worker.on('message', (msg) => {
    switch (msg.type) {
      case 'output':
        outputPass.push(Buffer.from(msg.data));
        break;
      case 'done':
        outputPass.push(null);  // EOF
        worker.terminate();
        break;
      case 'error':
        outputPass.destroy(new Error(msg.message));
        worker.terminate();
        break;
    }
  });

  worker.postMessage({ type: 'convert', input: inputBytes.buffer }, [inputBytes.buffer]);

  return { outputStream: outputPass, worker };
}
