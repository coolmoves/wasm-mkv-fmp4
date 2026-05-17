/**
 * ffmpeg-client.js
 * Host-side API for the ffmpeg.worker.js streaming MKV → fMP4 converter.
 *
 * Usage (browser or Worker):
 *
 *   import { FFmpegClient } from './ffmpeg-client.js';
 *
 *   const client = new FFmpegClient({ workerURL: '/dist/ffmpeg.worker.js' });
 *   await client.load();
 *
 *   // ── Option A: Convert a complete file (Uint8Array) ──────────────────────
 *   const fmp4 = await client.convertFile(mkvBytes);
 *
 *   // ── Option B: Streaming / chunk-by-chunk ────────────────────────────────
 *   const { write, end, result } = client.startStream();
 *   for await (const chunk of yourMkvSource) write(chunk);
 *   end();
 *   const outputChunks = await result; // Array<Uint8Array>
 *
 *   // ── Option C: Event-based (lower level) ────────────────────────────────
 *   client.on('output',   (chunk)        => ...);
 *   client.on('progress', ({ratio,time}) => ...);
 *   client.on('log',      ({level,msg})  => ...);
 *   client.on('done',     ()             => ...);
 *   client.on('error',    (message)      => ...);
 *   await client.start();
 *   client.write(chunk1);
 *   client.write(chunk2);
 *   client.end();
 */

'use strict';

export class FFmpegClient {
  /**
   * @param {object} opts
   * @param {string}  opts.workerURL  - URL of ffmpeg.worker.js
   * @param {string} [opts.coreURL]   - URL of ffmpeg-core.js  (default: same dir as workerURL)
   * @param {string} [opts.wasmURL]   - URL of ffmpeg-core.wasm (default: same dir as workerURL)
   */
  constructor({ workerURL, coreURL, wasmURL } = {}) {
    if (!workerURL) throw new Error('workerURL is required');
    this._workerURL = workerURL;
    this._coreURL = coreURL || resolveURL('ffmpeg-core.js', workerURL);
    this._wasmURL = wasmURL || resolveURL('ffmpeg-core.wasm', workerURL);
    this._worker = null;
    this._handlers = {};   // event name → [callback]
    this._ready = false;
  }

  /* ───────────────────────────────── Lifecycle ────────────────────────────── */

  /** Spawn worker and wait until WASM is loaded */
  load() {
    return new Promise((resolve, reject) => {
      this._worker = new Worker(this._workerURL);
      this._worker.onmessage = (evt) => this._dispatch(evt.data);
      this._worker.onerror   = (err) => reject(err);

      this.once('ready', resolve);
      this.once('error', reject);

      this._worker.postMessage({
        type:    'init',
        coreURL: this._coreURL,
        wasmURL: this._wasmURL,
      });
    });
  }

  /** Terminate the worker */
  terminate() {
    if (this._worker) this._worker.terminate();
    this._worker = null;
    this._ready  = false;
  }

  /* ────────────────────────────── High-level API ───────────────────────────── */

  /**
   * Convert a complete MKV buffer to fMP4.
   * @param {Uint8Array|ArrayBuffer} mkvData
   * @returns {Promise<Uint8Array>} concatenated fMP4 bytes
   */
  convertFile(mkvData) {
    return new Promise((resolve, reject) => {
      const outputChunks = [];
      this.on('output', (chunk) => outputChunks.push(chunk));
      this.once('done',  () => resolve(mergeChunks(outputChunks)));
      this.once('error', reject);

      const data = toUint8Array(mkvData);
      this._worker.postMessage({
        type:        'start',
        inputChunks: [data],
      }, [data.buffer]);
    });
  }

  /**
   * Begin a streaming conversion.
   * @returns {{ write(chunk), end(), result: Promise<Uint8Array[]> }}
   */
  startStream() {
    let resolve, reject;
    const result = new Promise((res, rej) => { resolve = res; reject = rej; });
    const outputChunks = [];

    this.on('output', (chunk) => outputChunks.push(chunk));
    this.once('done',  () => resolve(outputChunks));
    this.once('error', reject);

    // Kick off without pre-loaded data
    this._worker.postMessage({ type: 'start' });

    return {
      write: (chunk) => this.write(chunk),
      end:   ()      => this.end(),
      result,
    };
  }

  /* ──────────────────────────── Low-level streaming ───────────────────────── */

  /** Send a start signal (no pre-loaded chunks) */
  async start() {
    this._worker.postMessage({ type: 'start' });
  }

  /** Push a chunk of MKV data to the worker */
  write(chunk) {
    const data = toUint8Array(chunk);
    this._worker.postMessage({ type: 'chunk', data }, [data.buffer]);
  }

  /** Signal end-of-input */
  end() {
    this._worker.postMessage({ type: 'end' });
  }

  /** Abort the current conversion */
  abort() {
    this._worker.postMessage({ type: 'abort' });
  }

  /* ─────────────────────────────── Event emitter ───────────────────────────── */

  /**
   * Register a persistent listener.
   * Events: 'ready' | 'output' | 'progress' | 'log' | 'done' | 'error'
   */
  on(event, handler) {
    (this._handlers[event] ||= []).push({ handler, once: false });
    return this;
  }

  /** Register a one-time listener */
  once(event, handler) {
    (this._handlers[event] ||= []).push({ handler, once: true });
    return this;
  }

  /** Remove a specific listener or all listeners for an event */
  off(event, handler) {
    if (!this._handlers[event]) return;
    if (handler) {
      this._handlers[event] = this._handlers[event].filter(e => e.handler !== handler);
    } else {
      delete this._handlers[event];
    }
    return this;
  }

  _dispatch(msg) {
    const { type, ...payload } = msg;
    const entries = this._handlers[type] || [];
    this._handlers[type] = entries.filter(e => !e.once);
    const arg = type === 'output'   ? payload.data
              : type === 'progress' ? { ratio: payload.ratio, time: payload.time }
              : type === 'log'      ? { level: payload.level, msg: payload.msg }
              : type === 'error'    ? payload.message
              : undefined;
    entries.forEach(({ handler }) => handler(arg));
  }
}

/* ─────────────────────────────────── Helpers ─────────────────────────────── */

function toUint8Array(data) {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  return new Uint8Array(data);
}

function mergeChunks(chunks) {
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.byteLength; }
  return out;
}

function resolveURL(filename, baseURL) {
  return new URL(filename, baseURL).href;
}
