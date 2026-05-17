
// Node.js compatible wrapper for ffmpeg-core.js
// This module adds the necessary polyfills before loading the WASM core.

// Polyfill 'self' for Node.js worker_threads environment
if (typeof self === 'undefined') {
  globalThis.self = globalThis;
}
if (typeof self.location === 'undefined') {
  self.location = { href: import.meta.url };
}
if (typeof performance === 'undefined') {
  globalThis.performance = { now: () => Date.now() };
}

export { default } from './ffmpeg-core.js';
