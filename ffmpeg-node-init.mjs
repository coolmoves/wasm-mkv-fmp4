// Node.js polyfills required before loading ffmpeg-core.js
if (typeof self === 'undefined') globalThis.self = globalThis;
if (typeof self.location === 'undefined') self.location = { href: import.meta.url };
if (typeof performance === 'undefined') globalThis.performance = { now: () => Date.now() };
