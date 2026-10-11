import assert from 'node:assert';
import { Buffer } from 'node:buffer';
import { LockFreeRingBuffer, nextPowerOfTwo } from '../src/core.js';

console.log('Running ChronoRing smoke test suite...');

// 1. Math power-of-two rounding test
assert.strictEqual(nextPowerOfTwo(3), 4);
assert.strictEqual(nextPowerOfTwo(4), 4);
assert.strictEqual(nextPowerOfTwo(1000), 1024);
assert.strictEqual(nextPowerOfTwo(65000), 65536);
console.log('✓ Power-of-two alignment assertions passed');

// 2. Buffer initialization and capacity sanity
const ring = new LockFreeRingBuffer({ capacity: 8, slotPayloadSize: 64 });
assert.strictEqual(ring.getCapacity(), 8);
assert.strictEqual(ring.getAvailableToRead(), 0);
assert.strictEqual(ring.getAvailableToWrite(), 8);
console.log('✓ Initial capacity and occupancy assertions passed');

// 3. FIFO ordering & Monotonic sequence assertions
const msg1 = Buffer.from('Alpha-1234');
const msg2 = Buffer.from('Beta-5678');

const seq1 = ring.write(msg1);
const seq2 = ring.write(msg2);
assert.strictEqual(seq1, 0n);
assert.strictEqual(seq2, 1n);
assert.strictEqual(ring.getAvailableToRead(), 2);

const out1 = ring.read();
assert.ok(out1 !== null);
assert.strictEqual(out1.sequence, 0n);
assert.strictEqual(Buffer.from(out1.payload).toString(), 'Alpha-1234');

const out2 = ring.read();
assert.ok(out2 !== null);
assert.strictEqual(out2.sequence, 1n);
assert.strictEqual(Buffer.from(out2.payload).toString(), 'Beta-5678');

assert.strictEqual(ring.read(), null, 'Ring buffer should now be empty');
console.log('✓ Monotonic sequencing and payload integrity verified');

// 4. Wrapping and saturation test
for (let i = 0; i < 8; i++) {
  const s = ring.write(Buffer.from(`item-${i}`));
  assert.strictEqual(s, BigInt(i + 2));
}
// Ring should be completely full (capacity 8 reached)
const overflow = ring.write(Buffer.from('overflow'));
assert.strictEqual(overflow, -1n, 'Full buffer must return -1n backpressure');

// Drain 1 item and write again to test pointer wrap-around
const drained = ring.read();
assert.ok(drained !== null);
assert.strictEqual(drained.sequence, 2n);

const wrapSeq = ring.write(Buffer.from('wrapped-entry'));
assert.strictEqual(wrapSeq, 10n, 'Wrapped write should receive sequence 10');

console.log('✓ Capacity saturation and modulo wrap-around verified');
console.log('\nAll smoke tests passed cleanly!');
