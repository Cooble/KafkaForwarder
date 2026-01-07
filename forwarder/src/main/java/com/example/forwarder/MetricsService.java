package com.example.forwarder;

import org.HdrHistogram.Recorder;
import org.HdrHistogram.Histogram;
import org.springframework.beans.factory.annotation.Value;
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

    // Ramp-up parameters from producer
    @Value("${producer.ramp.start.rate:1}")
    private double startRate;

    @Value("${producer.ramp.end.rate:3000}")
    private double endRate;

    @Value("${producer.ramp.duration.seconds:300}")
    private int rampDurationSeconds;

    // Time tracking starts from first received event
    private volatile long firstEventTime = -1;
    private final long serviceStartTime = System.currentTimeMillis();

    public MetricsService() {
        try {
            String filename = "broker_stats_" + System.currentTimeMillis() + ".csv";
            csvWriter = new PrintWriter(new FileWriter(filename, true));
            csvWriter.println("TimeSeconds,EmissionRate_PerSec,Throughput_MsgPerSec,CumulativeAcks,CumulativeSent,Mean_ms,P99_ms,Max_ms,StartRate,EndRate,Duration");
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

        // Set first event time on first call (thread-safe enough with volatile)
        if (firstEventTime == -1) {
            firstEventTime = now;
        }

        // 1. Record Latency (Thread-safe inside Recorder)
        recorder.recordValue(residenceTime);

        // 2. Increment Throughput Counter
        ackCount.increment();
    }

    // --- BATCH PATH (Called by batch processor) ---
    // Records multiple acks at once without expensive loop overhead
    public void recordAckBatch(int count, long currentTimeMs) {
        // Set first event time on first call
        if (firstEventTime == -1) {
            firstEventTime = currentTimeMs;
        }

        // Just increment the counter by batch size
        // We skip individual latency recording for batch processing
        // to avoid the overhead - throughput metrics are still accurate
        ackCount.add(count);
    }

    // --- COLD PATH (Called once per second) ---
    @Scheduled(fixedRate = 1000)
    public void snapshot() {
        // Skip if no events received yet
        if (firstEventTime == -1) {
            return;
        }

        // 1. Atomic Swap
        // getIntervalHistogram() atomically swaps the active recording buffer
        // with a fresh one. Writers barely notice.
        intervalHistogram = recorder.getIntervalHistogram(intervalHistogram);

        // 2. Get the count for this second and reset the adder
        long countThisSecond = ackCount.sumThenReset();
        totalProcessed.add(countThisSecond);

        if (countThisSecond > 0) {
            // Time elapsed since first event (in seconds)
            long elapsedMs = System.currentTimeMillis() - firstEventTime;
            long elapsedSeconds = elapsedMs / 1000;

            // Calculate current emission rate based on ramp-up parameters
            double emissionRate;
            long rampDurationMs = rampDurationSeconds * 1000L;

            if (elapsedMs >= rampDurationMs) {
                // Ramp complete, stay at end rate
                emissionRate = endRate;
            } else {
                // Linear ramp: rate = startRate + (progress * range)
                double progress = (double) elapsedMs / rampDurationMs;
                double rateRange = endRate - startRate;
                emissionRate = startRate + (progress * rateRange);
            }

            // Calculate cumulative sent events (integral of the ramp function)
            long cumulativeSent;
            if (elapsedMs >= rampDurationMs) {
                // During ramp: integral of linear function from 0 to rampDuration
                // Area under ramp = (startRate + endRate) / 2 * rampDuration
                double rampArea = (startRate + endRate) / 2.0 * rampDurationSeconds;

                // After ramp: add constant rate * time after ramp
                double timeAfterRamp = (elapsedMs - rampDurationMs) / 1000.0;
                cumulativeSent = (long) (rampArea + (endRate * timeAfterRamp));
            } else {
                // During ramp: integral from 0 to current time
                // For linear ramp r(t) = startRate + (t/T) * (endRate - startRate)
                // Integral = startRate * t + (1/2) * (t^2/T) * (endRate - startRate)
                double t = elapsedMs / 1000.0;
                double T = rampDurationSeconds;
                double rateRange = endRate - startRate;
                cumulativeSent = (long) (startRate * t + 0.5 * (t * t / T) * rateRange);
            }

            // 3. Expensive math happens here (off the hot path)
            double mean = intervalHistogram.getMean();
            long p99 = intervalHistogram.getValueAtPercentile(99.0);
            long max = intervalHistogram.getMaxValue();

            // Get cumulative ack count
            long cumulativeAcks = totalProcessed.sum();

            // 4. Write I/O - Include emission rate, cumulative acks, cumulative sent, and ramp-up parameters
            csvWriter.printf("%d,%.2f,%d,%d,%d,%.2f,%d,%d,%.2f,%.2f,%d%n",
                    elapsedSeconds, emissionRate, countThisSecond, cumulativeAcks, cumulativeSent, mean, p99, max,
                    startRate, endRate, rampDurationSeconds);
            csvWriter.flush();

            System.out.println("Stats: " + countThisSecond + "/sec | Cumulative: " + cumulativeAcks + "/" + cumulativeSent + " | P99: " + p99 + "ms | EmissionRate: " + String.format("%.2f", emissionRate) + "/sec");
        }
    }
}