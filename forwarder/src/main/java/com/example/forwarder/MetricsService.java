package com.example.forwarder;

import org.HdrHistogram.Recorder;
import org.HdrHistogram.Histogram;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.concurrent.atomic.LongAdder;

@Service
public class MetricsService {

    // LongAdder is much faster than AtomicLong under high contention
    // It keeps separate counters per thread and sums them only when asked.
    private final LongAdder ackCount = new LongAdder();
    private final LongAdder totalProcessed = new LongAdder();

    // The "Recorder" handles the concurrency and buffering for us.
    // It is designed specifically for high-throughput recording.
    private final Recorder recorder = new Recorder(3600000000L, 3);

    // We reuse this histogram instance to avoid garbage collection
    private Histogram intervalHistogram = null;

    private PrintWriter csvWriter;
    private final long startTime = System.currentTimeMillis();

    public MetricsService() {
        try {
            String filename = "broker_stats_" + System.currentTimeMillis() + ".csv";
            csvWriter = new PrintWriter(new FileWriter(filename, true));
            csvWriter.println("TimeSeconds,Throughput_MsgPerSec,Mean_ms,P99_ms,Max_ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // --- HOT PATH (Called by thousands of threads) ---
    // NO 'synchronized'. NO blocking.
    public void recordAck(long ingestTimeMs) {
        long now = System.currentTimeMillis();
        long residenceTime = now - ingestTimeMs;

        if (residenceTime < 0) residenceTime = 0;

        // 1. Record Latency (Thread-safe inside Recorder)
        recorder.recordValue(residenceTime);

        // 2. Increment Throughput Counter
        ackCount.increment();
    }

    // --- COLD PATH (Called once per second) ---
    @Scheduled(fixedRate = 1000)
    public void snapshot() {
        // 1. Atomic Swap
        // getIntervalHistogram() atomically swaps the active recording buffer
        // with a fresh one. Writers barely notice.
        intervalHistogram = recorder.getIntervalHistogram(intervalHistogram);

        // 2. Get the count for this second and reset the adder
        long countThisSecond = ackCount.sumThenReset();
        totalProcessed.add(countThisSecond);

        if (countThisSecond > 0) {
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;

            // 3. Expensive math happens here (off the hot path)
            double mean = intervalHistogram.getMean();
            long p99 = intervalHistogram.getValueAtPercentile(99.0);
            long max = intervalHistogram.getMaxValue();

            // 4. Write I/O
            csvWriter.printf("%d,%d,%.2f,%d,%d%n",
                    elapsed, countThisSecond, mean, p99, max);
            csvWriter.flush();

            System.out.println("Stats: " + countThisSecond + "/sec | P99: " + p99 + "ms");
        }
    }
}