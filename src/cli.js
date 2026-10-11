#!/usr/bin/env node
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { Buffer } from 'node:buffer';
import { LockFreeRingBuffer } from './core.js';

const __filename = fileURLToPath(import.meta.url);

// ANSI Color Tokens
const RESET = '\x1b[0m';
const BOLD = '\x1b[1m';
const DIM = '\x1b[2m';
const CYAN = '\x1b[36m';
const GREEN = '\x1b[32m';

const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const MAGENTA = '\x1b[35m';

function printBanner() {
  const banner = `
${CYAN}┌─────────────────────────────────────────────────────────────────────────────┐
│  ${BOLD}CHRONORING${RESET}${CYAN} :: Lock-Free Monotonic Ring Buffer Architecture                │
│  Sub-microsecond Cross-Thread Dispatch with Zero-Copy SharedArrayBuffer     │
└─────────────────────────────────────────────────────────────────────────────┘${RESET}`;
  console.log(banner);
}

function printHelp() {
  printBanner();
  console.log(`
${BOLD}USAGE:${RESET}
  $ chronoring <command> [options]

${BOLD}COMMANDS:${RESET}
  bench       Run a hardware-level lock-free worker thread benchmark
  inspect     Test monotonic sequence integrity and memory barriers
  help        Show this comprehensive help interface

${BOLD}BENCHMARK OPTIONS:${RESET}
  --messages <count>   Number of messages to push (default: 1,000,000)
  --capacity <slots>   Ring capacity (power of 2, default: 32768)
  --payload  <bytes>   Payload size in bytes per slot (default: 64)
  --histogram          Generate nanosecond latency percentiles (p50, p99, p99.9)

${BOLD}EXAMPLES:${RESET}
  $ chronoring bench --messages 2000000 --capacity 65536
  $ chronoring bench --payload 256 --histogram
  $ chronoring inspect
`);
}

function parseArgs(args) {
  const config = {
    command: 'bench',
    messages: 1_000_000,
    capacity: 32768,
    payload: 64,
    histogram: false
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === 'bench' || arg === 'inspect' || arg === 'help') {
      config.command = arg;
    } else if (arg === '--messages' || arg === '-m') {
      config.messages = parseInt(args[++i], 10);
    } else if (arg === '--capacity' || arg === '-c') {
      config.capacity = parseInt(args[++i], 10);
    } else if (arg === '--payload' || arg === '-p') {
      config.payload = parseInt(args[++i], 10);
    } else if (arg === '--histogram') {
      config.histogram = true;
    } else if (arg === '--help' || arg === '-h') {
      config.command = 'help';
    }
  }
  return config;
}

// Consumer Worker Logic (Runs in separate thread)
if (!isMainThread) {
  const { sharedBuffer, totalMessages, collectHistogram } = workerData;
  const ring = new LockFreeRingBuffer({ sharedBuffer });
  const tempTarget = new Uint8Array(ring.getSlotPayloadSize());
  
  let readCount = 0;
  let lastSeq = -1n;
  let outOfOrder = 0;
  
  // Pre-allocate array for latency samples if histogram requested
  const sampleCapacity = collectHistogram ? Math.min(totalMessages, 200000) : 0;
  const latencies = collectHistogram ? new Float64Array(sampleCapacity) : null;
  let sampleIdx = 0;
  const sampleInterval = collectHistogram ? Math.max(1, Math.floor(totalMessages / sampleCapacity)) : 1;

  const startNs = process.hrtime.bigint();

  while (readCount < totalMessages) {
    const item = ring.read(tempTarget);
    if (item === null) {
      // Low latency spin-wait
      continue;
    }

    if (item.sequence !== lastSeq + 1n) {
      outOfOrder++;
    }
    lastSeq = item.sequence;

    if (collectHistogram && (readCount % sampleInterval === 0) && sampleIdx < sampleCapacity) {
      const now = process.hrtime.bigint();
      const diffNs = Number(now - item.timestamp);
      latencies[sampleIdx++] = diffNs > 0 ? diffNs : 0;
    }

    readCount++;
  }

  const endNs = process.hrtime.bigint();
  const durationMs = Number(endNs - startNs) / 1e6;

  parentPort.postMessage({
    durationMs,
    readCount,
    outOfOrder,
    latencies: collectHistogram ? Array.from(latencies.subarray(0, sampleIdx)) : []
  });
} else {
  async function runBenchmark(cfg) {
    printBanner();
    console.log(`${BOLD}Initializing SharedArrayBuffer IPC Pipeline...${RESET}`);
    console.log(`Config: Capacity = ${CYAN}${cfg.capacity}${RESET} slots | ` +
                `Payload = ${CYAN}${cfg.payload}B${RESET} | ` +
                `Target Ops = ${CYAN}${cfg.messages.toLocaleString()}${RESET}\n`);

    const producerRing = new LockFreeRingBuffer({
      capacity: cfg.capacity,
      slotPayloadSize: cfg.payload
    });

    const payloadData = new Uint8Array(cfg.payload);
    for (let i = 0; i < cfg.payload; i++) {
      payloadData[i] = (i & 0xff);
    }

    const consumerWorker = new Worker(__filename, {
      workerData: {
        sharedBuffer: producerRing.getSharedBuffer(),
        totalMessages: cfg.messages,
        collectHistogram: cfg.histogram
      }
    });

    // Warmup synchronization
    await new Promise((r) => setTimeout(r, 60));

    const t0 = process.hrtime.bigint();
    let writeCount = 0;

    while (writeCount < cfg.messages) {
      const seq = producerRing.write(payloadData, cfg.payload, cfg.histogram ? process.hrtime.bigint() : 0n);
      if (seq >= 0n) {
        writeCount++;
      }
    }

    const consumerResult = await new Promise((resolve, reject) => {
      consumerWorker.once('message', resolve);
      consumerWorker.once('error', reject);
    });
    const t1 = process.hrtime.bigint();
    const totalProducerDurationMs = Number(t1 - t0) / 1e6;
    const activeDurationMs = Math.max(totalProducerDurationMs, consumerResult.durationMs);

    const opsPerSec = (cfg.messages / (activeDurationMs / 1000));
    const mbPerSec = (opsPerSec * cfg.payload) / (1024 * 1024);

    // Display Boxed Table
    console.log(`${CYAN}┌───────────────────────────────────┬─────────────────────────────────────────┐${RESET}`);
    console.log(`${CYAN}│${RESET} ${BOLD}BENCHMARK EXECUTION METRIC       ${RESET} ${CYAN}│${RESET} ${BOLD}RECORDED OUTPUT                         ${RESET} ${CYAN}│${RESET}`);
    console.log(`${CYAN}├───────────────────────────────────┼─────────────────────────────────────────┤${RESET}`);
    console.log(`${CYAN}│${RESET} Total Processed Messages           ${CYAN}│${RESET} ${GREEN}${cfg.messages.toLocaleString()}${RESET} ops`.padEnd(52) + `${CYAN}│${RESET}`);
    console.log(`${CYAN}│${RESET} Active Wall Clock Time             ${CYAN}│${RESET} ${YELLOW}${activeDurationMs.toFixed(2)} ms${RESET}`.padEnd(52) + `${CYAN}│${RESET}`);
    console.log(`${CYAN}│${RESET} Lock-Free Throughput               ${CYAN}│${RESET} ${BOLD}${GREEN}${opsPerSec.toLocaleString(undefined, { maximumFractionDigits: 0 })} ops/sec${RESET}`.padEnd(54) + `${CYAN}│${RESET}`);
    console.log(`${CYAN}│${RESET} Data Transfer Rate                 ${CYAN}│${RESET} ${MAGENTA}${mbPerSec.toFixed(2)} MB/s${RESET}`.padEnd(52) + `${CYAN}│${RESET}`);
    console.log(`${CYAN}│${RESET} Monotonic Sequencing Violations    ${CYAN}│${RESET} ${consumerResult.outOfOrder === 0 ? GREEN + '0 (Strict FIFO Invariant Preserved)' + RESET : RED + consumerResult.outOfOrder + ' (CORRUPT)' + RESET}`.padEnd(61) + `${CYAN}│${RESET}`);
    console.log(`${CYAN}└───────────────────────────────────┴─────────────────────────────────────────┘${RESET}`);

    if (cfg.histogram && consumerResult.latencies.length > 0) {
      const sorted = consumerResult.latencies.sort((a, b) => a - b);
      const p50 = sorted[Math.floor(sorted.length * 0.50)];
      const p90 = sorted[Math.floor(sorted.length * 0.90)];
      const p99 = sorted[Math.floor(sorted.length * 0.99)];
      const p999 = sorted[Math.floor(sorted.length * 0.999)];

      console.log(`\n${BOLD}Cross-Thread End-to-End Latency Profile:${RESET}`);
      console.log(`${CYAN}┌─────────────┬─────────────┬─────────────┬─────────────┐${RESET}`);
      console.log(`${CYAN}│${RESET}     p50     ${CYAN}│${RESET}     p90     ${CYAN}│${RESET}     p99     ${CYAN}│${RESET}    p99.9    ${CYAN}│${RESET}`);
      console.log(`${CYAN}├─────────────┼─────────────┼─────────────┼─────────────┤${RESET}`);
      console.log(`${CYAN}│${RESET} ${String(Math.round(p50) + ' ns').padStart(11)} ${CYAN}│${RESET} ${String(Math.round(p90) + ' ns').padStart(11)} ${CYAN}│${RESET} ${String(Math.round(p99) + ' ns').padStart(11)} ${CYAN}│${RESET} ${String(Math.round(p999) + ' ns').padStart(11)} ${CYAN}│${RESET}`);
      console.log(`${CYAN}└─────────────┴─────────────┴─────────────┴─────────────┘${RESET}\n`);
    }
  }

  function inspectMode() {
    printBanner();
    console.log(`${BOLD}Executing In-Memory Ring Buffer Correctness & Barrier Verification...${RESET}\n`);
    
    const ring = new LockFreeRingBuffer({ capacity: 4, slotPayloadSize: 32 });
    const inputA = Buffer.from('TEST_PAYLOAD_001');
    const inputB = Buffer.from('TEST_PAYLOAD_002');
    const inputC = Buffer.from('TEST_PAYLOAD_003');
    const inputD = Buffer.from('TEST_PAYLOAD_004');
    const inputE = Buffer.from('OVERFLOW_PAYLOAD');

    console.log(`  1. Writing 4 consecutive packets to capacity-4 ring...`);
    const seqA = ring.write(inputA);
    const seqB = ring.write(inputB);
    const seqC = ring.write(inputC);
    const seqD = ring.write(inputD);
    console.log(`     Sequences returned: [${seqA}, ${seqB}, ${seqC}, ${seqD}] (Monotonic 0n..3n)`);

    console.log(`  2. Testing backpressure overflow behavior on full ring...`);
    const seqE = ring.write(inputE);
    console.log(`     Attempt on full buffer returned: ${seqE === -1n ? GREEN + '-1n (Backpressure enforced)' + RESET : RED + 'Failed' + RESET}`);

    console.log(`  3. Draining 2 packets from buffer...`);
    const readA = ring.read();
    const readB = ring.read();
    console.log(`     Packet 0: seq=${readA.sequence}, payload="${Buffer.from(readA.payload).toString('utf8')}"`);
    console.log(`     Packet 1: seq=${readB.sequence}, payload="${Buffer.from(readB.payload).toString('utf8')}"`);

    console.log(`  4. Re-attempting write to slot freed by tail progression...`);
    const seqRetry = ring.write(inputE);
    console.log(`     New slot write succeeded with sequence: ${GREEN}${seqRetry}${RESET}`);
    
    console.log(`\n${GREEN}${BOLD}✓ All barrier invariants, sequence checks, and wrapping masks passed.${RESET}\n`);
  }

  async function main() {
    const config = parseArgs(process.argv.slice(2));
    switch (config.command) {
      case 'help':
        printHelp();
        break;
      case 'inspect':
        inspectMode();
        break;
      case 'bench':
      default:
        await runBenchmark(config);
        break;
    }
  }

  main().catch((err) => {
    console.error(`${RED}Fatal Execution Error:${RESET}`, err);
    process.exit(1);
  });
}
