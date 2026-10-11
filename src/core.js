import { Buffer } from 'node:buffer';

/**
 * Layout of the Header in 64-bit BigInt words (each 8 bytes):
 * Index 0: Producer Head (Monotonic written sequence)
 * Index 1..7: Cache-line padding (64 bytes boundary)
 * Index 8: Consumer Tail (Monotonic read sequence)
 * Index 9..15: Cache-line padding (64 bytes boundary)
 * Index 16: Ring Capacity (Slots count, must be power of 2)
 * Index 17: Slot Payload Size (Bytes)
 */
export const HEADER_OFFSETS = {
  HEAD_INDEX: 0,
  TAIL_INDEX: 8,
  CAPACITY_INDEX: 16,
  SLOT_PAYLOAD_INDEX: 17,
  TOTAL_HEADER_WORDS: 24
};

export const HEADER_BYTES = HEADER_OFFSETS.TOTAL_HEADER_WORDS * 8;

/**
 * Slot Layout:
 * [0..7]   : 64-bit Monotonic Sequence Number
 * [8..15]  : 64-bit Producer Timestamp (nanoseconds from hrtime.bigint())
 * [16..19] : 32-bit Payload length
 * [20..23] : Reserved/Flags
 * [24..N]  : Raw byte payload
 */
export const SLOT_HEADER_BYTES = 24;

/**
 * Ensures capacity is a power of 2 for fast bitmask wrapping.
 */
export function nextPowerOfTwo(value) {
  let v = Math.max(2, value >>> 0);
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  return v + 1;
}

/**
 * LockFreeRingBuffer provides a single-producer single-consumer (SPSC) or 
 * multi-thread coordinated ring buffer over SharedArrayBuffer.
 */
export class LockFreeRingBuffer {
  constructor(options = {}) {
    const { sharedBuffer, capacity = 1024, slotPayloadSize = 128 } = options;

    if (sharedBuffer) {
      this.buffer = sharedBuffer;
      this.header = new BigInt64Array(this.buffer, 0, HEADER_OFFSETS.TOTAL_HEADER_WORDS);
      this.capacity = Number(this.header[HEADER_OFFSETS.CAPACITY_INDEX]);
      this.slotPayloadSize = Number(this.header[HEADER_OFFSETS.SLOT_PAYLOAD_INDEX]);
    } else {
      this.capacity = nextPowerOfTwo(capacity);
      this.slotPayloadSize = (slotPayloadSize + 7) & ~7; // 8-byte aligned
      this.slotStride = SLOT_HEADER_BYTES + this.slotPayloadSize;
      const totalBufferSize = HEADER_BYTES + (this.capacity * this.slotStride);
      
      this.buffer = new SharedArrayBuffer(totalBufferSize);
      this.header = new BigInt64Array(this.buffer, 0, HEADER_OFFSETS.TOTAL_HEADER_WORDS);
      this.header[HEADER_OFFSETS.HEAD_INDEX] = 0n;
      this.header[HEADER_OFFSETS.TAIL_INDEX] = 0n;
      this.header[HEADER_OFFSETS.CAPACITY_INDEX] = BigInt(this.capacity);
      this.header[HEADER_OFFSETS.SLOT_PAYLOAD_INDEX] = BigInt(this.slotPayloadSize);
    }

    this.mask = BigInt(this.capacity - 1);
    this.slotStride = SLOT_HEADER_BYTES + this.slotPayloadSize;
    this.slotsData = new Uint8Array(this.buffer, HEADER_BYTES);
    this.slotsDataView = new DataView(this.buffer, HEADER_BYTES);
  }

  getSharedBuffer() {
    return this.buffer;
  }

  getCapacity() {
    return this.capacity;
  }

  getSlotPayloadSize() {
    return this.slotPayloadSize;
  }

  getAvailableToRead() {
    const head = Atomics.load(this.header, HEADER_OFFSETS.HEAD_INDEX);
    const tail = Atomics.load(this.header, HEADER_OFFSETS.TAIL_INDEX);
    return Number(head - tail);
  }

  getAvailableToWrite() {
    const head = Atomics.load(this.header, HEADER_OFFSETS.HEAD_INDEX);
    const tail = Atomics.load(this.header, HEADER_OFFSETS.TAIL_INDEX);
    return this.capacity - Number(head - tail);
  }

  /**
   * Lock-free write into the ring buffer.
   * Returns assigned sequence ID (BigInt) on success, or -1n if full.
   */
  write(payloadBuffer, length = payloadBuffer.byteLength, timestamp = process.hrtime.bigint()) {
    const head = Atomics.load(this.header, HEADER_OFFSETS.HEAD_INDEX);
    const tail = Atomics.load(this.header, HEADER_OFFSETS.TAIL_INDEX);

    if (head - tail >= BigInt(this.capacity)) {
      return -1n; // Ring buffer full
    }

    const slotIndex = Number(head & this.mask);
    const slotByteOffset = slotIndex * this.slotStride;

    // Write Slot Header: [seq: 64b, timestamp: 64b, len: 32b, flags: 32b]
    this.slotsDataView.setBigInt64(slotByteOffset, head, true);
    this.slotsDataView.setBigInt64(slotByteOffset + 8, timestamp, true);
    const writeLen = Math.min(length, this.slotPayloadSize);
    this.slotsDataView.setUint32(slotByteOffset + 16, writeLen, true);
    this.slotsDataView.setUint32(slotByteOffset + 20, 0, true);

    // Zero-copy or slice write into slot
    if (payloadBuffer instanceof Uint8Array) {
      this.slotsData.set(
        payloadBuffer.subarray(0, writeLen),
        slotByteOffset + SLOT_HEADER_BYTES
      );
    } else if (Buffer.isBuffer(payloadBuffer)) {
      this.slotsData.set(
        new Uint8Array(payloadBuffer.buffer, payloadBuffer.byteOffset, writeLen),
        slotByteOffset + SLOT_HEADER_BYTES
      );
    }

    // Atomic store with release semantics: commit head advance
    Atomics.store(this.header, HEADER_OFFSETS.HEAD_INDEX, head + 1n);
    return head;
  }

  /**
   * Lock-free read from the ring buffer into an optional target buffer.
   * Returns slot descriptor object, or null if empty.
   */
  read(targetBuffer = null) {
    const tail = Atomics.load(this.header, HEADER_OFFSETS.TAIL_INDEX);
    const head = Atomics.load(this.header, HEADER_OFFSETS.HEAD_INDEX);

    if (tail >= head) {
      return null; // Ring buffer empty
    }

    const slotIndex = Number(tail & this.mask);
    const slotByteOffset = slotIndex * this.slotStride;

    const seq = this.slotsDataView.getBigInt64(slotByteOffset, true);
    const timestamp = this.slotsDataView.getBigInt64(slotByteOffset + 8, true);
    const length = this.slotsDataView.getUint32(slotByteOffset + 16, true);

    let payload;
    const payloadStart = slotByteOffset + SLOT_HEADER_BYTES;
    if (targetBuffer && targetBuffer.byteLength >= length) {
      targetBuffer.set(this.slotsData.subarray(payloadStart, payloadStart + length));
      payload = targetBuffer.subarray(0, length);
    } else {
      // Allocating slice only if no target preallocated buffer provided
      payload = this.slotsData.slice(payloadStart, payloadStart + length);
    }

    // Atomic store with release semantics: commit tail advance
    Atomics.store(this.header, HEADER_OFFSETS.TAIL_INDEX, tail + 1n);

    return {
      sequence: seq,
      timestamp,
      length,
      payload
    };
  }
}
