package com.example.forwarder;

import com.example.forwarder.db.DeliveryStatusRepository;
import org.HdrHistogram.Recorder;
import org.HdrHistogram.Histogram;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.concurrent.atomic.LongAdder;

@Service
public class MetricsService {

    // === ACTUAL MEASURED METRICS (Real data from the system) ===
    // LongAdder is much faster than AtomicLong under high contention
    // It keeps separate counters per thread and sums them only when asked.
    private final LongAdder actualKafkaReceivedThisSecond = new LongAdder();  // Events received from Kafka
    private final LongAdder actualKafkaReceivedTotal = new LongAdder();       // Total events from Kafka
    private final LongAdder actualAcksThisSecond = new LongAdder();
    private final LongAdder actualAcksTotal = new LongAdder();
    private final LongAdder actualSendAttemptsThisSecond = new LongAdder();
    private final LongAdder actualFailuresThisSecond = new LongAdder();
    private final LongAdder actualFailuresTotal = new LongAdder();  // FIX: Cumulative failures (never resets)

    // The "Recorder" handles the concurrency and buffering for us.
    // It is designed specifically for high-throughput recording.
    // Tracks ACTUAL latency from Kafka arrival to client ACK
    private final Recorder actualLatencyRecorder = new Recorder(3600000000L, 3);

    // We reuse this histogram instance to avoid garbage collection
    private Histogram intervalHistogram = null;

    private PrintWriter csvWriter;
    private String transportMode;

    @Autowired
    private DeliveryStatusRepository deliveryStatusRepository;

    // === SYNTHETIC/EXPECTED VALUES (Theoretical calculations based on producer config) ===
    // These are used to calculate what the producer SHOULD be emitting
    @Value("${producer.ramp.start.rate:1}")
    private double expectedStartRate;

    @Value("${producer.ramp.end.rate:3000}")
    private double expectedEndRate;

    @Value("${producer.ramp.duration.seconds:300}")
    private int expectedRampDurationSeconds;

    @Value("${producer.ramp.stop.after:true}")
    private boolean expectedStopAfterRamp;

    @Value("${forwarder.transport.mode:rest}")
    private String configuredTransportMode;

    // Time tracking starts from first received event
    private volatile long firstEventTime = -1;
    private final long serviceStartTime = System.currentTimeMillis();

    public MetricsService() {
        // Constructor - file creation happens in @PostConstruct after properties are injected
    }

    @jakarta.annotation.PostConstruct
    public void init() {
        try {
            transportMode = configuredTransportMode.toUpperCase();
            String timestamp = String.valueOf(System.currentTimeMillis());
            String filename = "metrics_" + transportMode + "_" + timestamp + ".csv";
            csvWriter = new PrintWriter(new FileWriter(filename, true));

            // Write configuration metadata as comments at the top of CSV
            csvWriter.println("# Metrics Configuration");
            csvWriter.println("# Transport Mode: " + transportMode);
            csvWriter.println("# Expected Producer Config:");
            csvWriter.println("#   Start Rate: " + expectedStartRate + " msg/sec");
            csvWriter.println("#   End Rate: " + expectedEndRate + " msg/sec");
            csvWriter.println("#   Ramp Duration: " + expectedRampDurationSeconds + " seconds");
            csvWriter.println("#   Stop After Ramp: " + expectedStopAfterRamp);
            csvWriter.println("# Timestamp: " + timestamp);
            csvWriter.println("#");

            // Clean CSV header with only time-series data (no constant columns)
            csvWriter.println("TimeSeconds,Expected_EmissionRate,Actual_KafkaReceived,Actual_Acks,Actual_SendAttempts,Actual_Failures,Actual_CumulativeKafkaReceived,Actual_CumulativeAcks,Expected_CumulativeSent,Actual_CumulativeFailures,Actual_PendingDeliveries,Actual_Lag_Behind,Actual_SuccessRate_%,Actual_MeanLatency_ms,Actual_P50_ms,Actual_P95_ms,Actual_P99_ms,Actual_P999_ms,Actual_Max_ms");
            csvWriter.flush();

            // Print configuration to console
            System.out.println("╔════════════════════════════════════════════════════════════════╗");
            System.out.println("║ MetricsService Initialized                                     ║");
            System.out.println("╠════════════════════════════════════════════════════════════════╣");
            System.out.println("║ Transport Mode:       " + String.format("%-36s", transportMode) + " ║");
            System.out.println("║ Expected Start Rate:  " + String.format("%-32.0f", expectedStartRate) + " msg/sec ║");
            System.out.println("║ Expected End Rate:    " + String.format("%-32.0f", expectedEndRate) + " msg/sec ║");
            System.out.println("║ Expected Ramp:        " + String.format("%-32d", expectedRampDurationSeconds) + " seconds ║");
            System.out.println("║ Stop After Ramp:      " + String.format("%-36s", expectedStopAfterRamp) + " ║");
            System.out.println("║ Metrics File:         " + String.format("%-36s", filename) + " ║");
            System.out.println("╚════════════════════════════════════════════════════════════════╝");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // --- HOT PATH (Called by thousands of threads) ---
    // NO 'synchronized'. NO blocking.
    public void recordAck(long ingestTimeMs) {
        long now = System.currentTimeMillis();
        long actualLatencyMs = now - ingestTimeMs;

        if (actualLatencyMs < 0) actualLatencyMs = 0;

        // Set first event time on first call (thread-safe enough with volatile)
        if (firstEventTime == -1) {
            firstEventTime = now;
        }

        // 1. Record ACTUAL Latency (Thread-safe inside Recorder)
        actualLatencyRecorder.recordValue(actualLatencyMs);

        // 2. Increment ACTUAL Throughput Counter
        actualAcksThisSecond.increment();
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
        actualAcksThisSecond.add(count);
    }

    // --- KAFKA TRACKING (Called by KafkaService when events arrive) ---
    public void recordKafkaReceived(int eventCount) {
        actualKafkaReceivedThisSecond.add(eventCount);
    }

    // --- SEND TRACKING (Called by SendService/WebSocketSendService) ---
    public void recordSendAttempt(int count) {
        actualSendAttemptsThisSecond.add(count);
    }

    public void recordSendFailure(int count) {
        actualFailuresThisSecond.add(count);
    }

    // --- COLD PATH (Called once per second) ---
    @Scheduled(fixedRate = 1000)
    public void snapshot() {
        // Skip if no events received yet
        if (firstEventTime == -1) {
            return;
        }

        // 1. Atomic Swap - Get ACTUAL latency histogram for this interval
        // getIntervalHistogram() atomically swaps the active recording buffer
        // with a fresh one. Writers barely notice.
        intervalHistogram = actualLatencyRecorder.getIntervalHistogram(intervalHistogram);

        // 2. Get ACTUAL counts for this second and reset the adders
        long actualKafkaReceived = actualKafkaReceivedThisSecond.sumThenReset();
        long actualAcks = actualAcksThisSecond.sumThenReset();
        long actualSendAttempts = actualSendAttemptsThisSecond.sumThenReset();
        long actualFailures = actualFailuresThisSecond.sumThenReset();

        actualKafkaReceivedTotal.add(actualKafkaReceived);
        actualAcksTotal.add(actualAcks);
        actualFailuresTotal.add(actualFailures);  // FIX: Accumulate failures properly

        // Time elapsed since first event (in seconds)
        long elapsedMs = System.currentTimeMillis() - firstEventTime;
        long elapsedSeconds = elapsedMs / 1000;

        // === SYNTHETIC CALCULATION: Expected emission rate based on producer config ===
        double syntheticEmissionRate;
        long rampDurationMs = expectedRampDurationSeconds * 1000L;

        if (elapsedMs >= rampDurationMs) {
            // Ramp complete - check if producer stops or continues at end rate
            syntheticEmissionRate = expectedStopAfterRamp ? 0 : expectedEndRate;
        } else {
            // Linear ramp: rate = startRate + (progress * range)
            double progress = (double) elapsedMs / rampDurationMs;
            double rateRange = expectedEndRate - expectedStartRate;
            syntheticEmissionRate = expectedStartRate + (progress * rateRange);
        }

        // === SYNTHETIC CALCULATION: Expected cumulative sent events (integral of ramp function) ===
        long syntheticCumulativeSent;
        if (elapsedMs >= rampDurationMs) {
            // Ramp completed - calculate total area under ramp
            double rampArea = (expectedStartRate + expectedEndRate) / 2.0 * expectedRampDurationSeconds;

            if (expectedStopAfterRamp) {
                // Producer stops after ramp - no additional messages
                syntheticCumulativeSent = (long) rampArea;
            } else {
                // Producer continues at end rate - add constant rate * time after ramp
                double timeAfterRamp = (elapsedMs - rampDurationMs) / 1000.0;
                syntheticCumulativeSent = (long) (rampArea + (expectedEndRate * timeAfterRamp));
            }
        } else {
            // During ramp: integral from 0 to current time
            // For linear ramp r(t) = startRate + (t/T) * (endRate - startRate)
            // Integral = startRate * t + (1/2) * (t^2/T) * (endRate - startRate)
            double t = elapsedMs / 1000.0;
            double T = expectedRampDurationSeconds;
            double rateRange = expectedEndRate - expectedStartRate;
            syntheticCumulativeSent = (long) (expectedStartRate * t + 0.5 * (t * t / T) * rateRange);
        }

        // 3. Calculate ACTUAL latency percentiles and stats
        double actualMeanLatency = intervalHistogram.getMean();
        long actualP50Latency = intervalHistogram.getValueAtPercentile(50.0);
        long actualP95Latency = intervalHistogram.getValueAtPercentile(95.0);
        long actualP99Latency = intervalHistogram.getValueAtPercentile(99.0);
        long actualP999Latency = intervalHistogram.getValueAtPercentile(99.9);
        long actualMaxLatency = intervalHistogram.getMaxValue();

        // Get ACTUAL cumulative counts
        long actualCumulativeKafkaReceived = actualKafkaReceivedTotal.sum();
        long actualCumulativeAcks = actualAcksTotal.sum();
        long actualCumulativeFailures = actualFailuresTotal.sum();  // FIX: Use the proper cumulative counter

        // Query DB for pending deliveries count (how healthy we are)
        long actualPendingDeliveries = deliveryStatusRepository.countPending();

        // Calculate lag: how far behind expected are we?
        long actualLagBehind = syntheticCumulativeSent - actualCumulativeAcks;

        // Calculate ACTUAL success rate
        double actualSuccessRate = actualSendAttempts > 0
            ? (actualAcks * 100.0 / actualSendAttempts)
            : 100.0;

        // 4. Write CSV with time-series data only (constants are in header comments)
        csvWriter.printf("%d,%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.2f,%.2f,%d,%d,%d,%d,%d%n",
                elapsedSeconds,                 // TimeSeconds
                syntheticEmissionRate,          // Expected emission rate (calculated from config)
                actualKafkaReceived,            // ACTUAL: Events received from Kafka this second
                actualAcks,                     // ACTUAL: Measured ACKs this second
                actualSendAttempts,             // ACTUAL: Measured send attempts this second
                actualFailures,                 // ACTUAL: Measured failures this second
                actualCumulativeKafkaReceived,  // ACTUAL: Total events received from Kafka
                actualCumulativeAcks,           // ACTUAL: Total ACKs received
                syntheticCumulativeSent,        // Expected total sent (calculated from config)
                actualCumulativeFailures,       // ACTUAL: Total failures (FIXED)
                actualPendingDeliveries,        // ACTUAL: Pending deliveries in DB (health indicator)
                actualLagBehind,                // ACTUAL: How many messages behind expected
                actualSuccessRate,              // ACTUAL: Success rate percentage
                actualMeanLatency,              // ACTUAL: Mean latency in ms
                actualP50Latency,               // ACTUAL: P50 latency
                actualP95Latency,               // ACTUAL: P95 latency
                actualP99Latency,               // ACTUAL: P99 latency
                actualP999Latency,              // ACTUAL: P99.9 latency
                actualMaxLatency);              // ACTUAL: Max latency this second
        csvWriter.flush();

        if (actualAcks > 0 || actualSendAttempts > 0) {
            System.out.printf("[%s] ACTUAL: %d acks/sec (%d sent, %d failed, %.1f%% success) | Total: %d acks vs %d expected (lag: %d) | Pending: %d | P99: %dms | Kafka: %d/sec%n",
                    transportMode, actualAcks, actualSendAttempts, actualFailures, actualSuccessRate,
                    actualCumulativeAcks, syntheticCumulativeSent, actualLagBehind, actualPendingDeliveries, actualP99Latency, actualKafkaReceived);
        }
    }
}

