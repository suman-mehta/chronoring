import { performance } from 'node:perf_hooks';
import { LockFreeRingBuffer } from '../src/core.js';

console.log('=== CHRONORING HIGH-RESOLUTION NANOSECOND IN-PROCESS BENCHMARK ===');

const ITERATIONS = 3_000_000;
const CAPACITY = 65536;
const PAYLOAD_SIZE = 64;

const ring = new LockFreeRingBuffer({
  capacity: CAPACITY,
  slotPayloadSize: PAYLOAD_SIZE
});

const testPayload = new Uint8Array(PAYLOAD_SIZE);
for (let i = 0; i < PAYLOAD_SIZE; i++) {
  testPayload[i] = i & 0xff;
}

const targetBuffer = new Uint8Array(PAYLOAD_SIZE);

console.log(`Warming up JIT optimizations with 100,000 passes...`);
for (let i = 0; i < 100000; i++) {
  ring.write(testPayload);
  ring.read(targetBuffer);
}

console.log(`Executing ${ITERATIONS.toLocaleString()} interleaved write & read operations...`);

const start = performance.now();

for (let i = 0; i < ITERATIONS; i++) {
  ring.write(testPayload);
  ring.read(targetBuffer);
}

const end = performance.now();
const durationMs = end - start;
const opsPerSec = (ITERATIONS / (durationMs / 1000));
const latencyPerOpNs = (durationMs * 1e6) / (ITERATIONS * 2); // 2 operations per loop (1 write, 1 read)

console.log('\n-----------------------------------------------------------');
console.log(`Total Elapsed Time   : ${durationMs.toFixed(2)} ms`);
console.log(`Throughput           : ${opsPerSec.toLocaleString(undefined, { maximumFractionDigits: 0 })} ops/sec`);
console.log(`Average Op Latency   : ${latencyPerOpNs.toFixed(2)} ns / operation`);
console.log('-----------------------------------------------------------');
