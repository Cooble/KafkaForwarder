package com.example.forwarder;

import com.example.common.RampupConfig;
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
    private final LongAdder actualKafkaAcceptedThisSecond = new LongAdder();  // Events persisted/accepted into DB
    private final LongAdder actualKafkaAcceptedTotal = new LongAdder();       // Cumulative accepted events
    private final LongAdder actualAcksThisSecond = new LongAdder();
    private final LongAdder actualAcksTotal = new LongAdder();
    private final LongAdder actualSendEventsThisSecond = new LongAdder();     // Events included in outbound sends
    private final LongAdder actualSendEventsTotal = new LongAdder();
    private final LongAdder actualSendAttemptsThisSecond = new LongAdder();
    private final LongAdder actualFailuresThisSecond = new LongAdder();
    private final LongAdder actualFailuresTotal = new LongAdder();  // FIX: Cumulative failures (never resets)
    private final LongAdder dbBatchCountThisSecond = new LongAdder();         // DB batches executed (acks persisted)
    private final LongAdder dbRowsUpdatedThisSecond = new LongAdder();        // Rows updated per second
    private final LongAdder dbTimeMsThisSecond = new LongAdder();             // Total DB time spent this second
    private final LongAdder dbRowsUpdatedTotal = new LongAdder();             // Cumulative rows updated
    private final LongAdder retryCyclesThisSecond = new LongAdder();          // Retry cycles executed
    private final LongAdder retryEventsPendingThisSecond = new LongAdder();   // Events considered for retry
    private final LongAdder retryEventsDispatchedThisSecond = new LongAdder();// Events actually re-dispatched
    private final LongAdder retrySkippedCapacityThisSecond = new LongAdder(); // Cycles skipped due to capacity
    private final LongAdder retryEventsDispatchedTotal = new LongAdder();     // Cumulative retried events sent

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
    // Loaded from rampup.properties
    @Value("${rampup.enabled:true}")
    private boolean rampupEnabled;

    @Value("${rampup.start.rate:1}")
    private double expectedStartRate;

    @Value("${rampup.end.rate:3000}")
    private double expectedEndRate;

    @Value("${rampup.duration.seconds:300}")
    private int expectedRampDurationSeconds;

    @Value("${rampup.stop.after:true}")
    private boolean expectedStopAfterRamp;

    @Value("${forwarder.transport.mode:rest}")
    private String configuredTransportMode;

    // Shared ramp-up calculation logic
    private RampupConfig rampupConfig;

    // Time tracking starts from first received event
    private volatile long firstEventTime = -1;
    private final long serviceStartTime = System.currentTimeMillis();

    public MetricsService() {
        // Constructor - file creation happens in @PostConstruct after properties are injected
    }

    @jakarta.annotation.PostConstruct
    public void init() {
        try {
            // Create shared ramp-up config
            rampupConfig = new RampupConfig(rampupEnabled, expectedStartRate, expectedEndRate,
                    expectedRampDurationSeconds, expectedStopAfterRamp);

            transportMode = configuredTransportMode.toUpperCase();
            String timestamp = String.valueOf(System.currentTimeMillis());
            String filename = "metrics_" + transportMode + "_" + timestamp + ".csv";
            csvWriter = new PrintWriter(new FileWriter(filename, true));

            // Write configuration metadata as comments at the top of CSV
            csvWriter.println("# Metrics Configuration");
            csvWriter.println("# Transport Mode: " + transportMode);
            csvWriter.println("# Rampup Config: " + rampupConfig);
            csvWriter.println("# Timestamp: " + timestamp);
            csvWriter.println("#");

            // Clean CSV header with only time-series data (no constant columns)
            csvWriter.println("TimeSeconds,Expected_EmissionRate,Actual_KafkaReceived,Actual_KafkaAccepted,Actual_SendEvents,Actual_Acks,Actual_SendAttempts,Actual_Failures,Actual_DbBatches,Actual_DbRowsUpdated,Actual_DbTimeMs,Actual_RetryCycles,Actual_RetryEvents,Actual_RetryDispatched,Actual_RetrySkippedCapacity,Actual_CumulativeKafkaReceived,Actual_CumulativeKafkaAccepted,Actual_CumulativeSendEvents,Actual_CumulativeAcks,Actual_CumulativeDbRowsUpdated,Actual_CumulativeRetryDispatched,Expected_CumulativeSent,Actual_CumulativeFailures,Actual_PendingDeliveries,Actual_Lag_Behind,Actual_SuccessRate_%,Actual_MeanLatency_ms,Actual_P50_ms,Actual_P95_ms,Actual_P99_ms,Actual_P999_ms,Actual_Max_ms");
            csvWriter.flush();

            // Print configuration to console
            System.out.println("╔════════════════════════════════════════════════════════════════╗");
            System.out.println("║ MetricsService Initialized                                     ║");
            System.out.println("╠════════════════════════════════════════════════════════════════╣");
            System.out.println("║ Transport Mode:       " + String.format("%-36s", transportMode) + " ║");
            System.out.println("║ Rampup Config:        " + String.format("%-36s", rampupConfig) + " ║");
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

    // --- KAFKA TRACKING (Called by KafkaService when events arrive) ---
    public void recordKafkaReceived(int eventCount) {
        actualKafkaReceivedThisSecond.add(eventCount);
    }

    // --- PERSIST TRACKING (Called after data is stored) ---
    public void recordKafkaAccepted(int eventCount) {
        actualKafkaAcceptedThisSecond.add(eventCount);
    }

    // --- SEND TRACKING (Called by SendService/WebSocketSendService) ---
    public void recordSendAttempt(int count) {
        actualSendAttemptsThisSecond.add(count);
    }

    public void recordSendEvents(int count) {
        actualSendEventsThisSecond.add(count);
    }

    public void recordSendFailure(int count) {
        actualFailuresThisSecond.add(count);
    }

    // --- RETRY TRACKING (Called by ResendService) ---
    public void recordRetryCycle(int pending, int dispatched, boolean skippedForCapacity) {
        retryCyclesThisSecond.increment();
        retryEventsPendingThisSecond.add(pending);
        retryEventsDispatchedThisSecond.add(dispatched);
        if (skippedForCapacity) {
            retrySkippedCapacityThisSecond.increment();
        }
    }

    // --- DB TRACKING (Called by DeliveryResultHandler when persisting ACKs) ---
    public void recordDbWrite(int rowsUpdated, long durationMs) {
        dbBatchCountThisSecond.increment();
        dbRowsUpdatedThisSecond.add(rowsUpdated);
        dbTimeMsThisSecond.add(Math.max(durationMs, 0));
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
        long actualKafkaAccepted = actualKafkaAcceptedThisSecond.sumThenReset();
        long actualSendEvents = actualSendEventsThisSecond.sumThenReset();
        long actualAcks = actualAcksThisSecond.sumThenReset();
        long actualSendAttempts = actualSendAttemptsThisSecond.sumThenReset();
        long actualFailures = actualFailuresThisSecond.sumThenReset();
        long dbBatches = dbBatchCountThisSecond.sumThenReset();
        long dbRowsUpdated = dbRowsUpdatedThisSecond.sumThenReset();
        long dbTimeMs = dbTimeMsThisSecond.sumThenReset();
        long retryCycles = retryCyclesThisSecond.sumThenReset();
        long retryEvents = retryEventsPendingThisSecond.sumThenReset();
        long retryDispatched = retryEventsDispatchedThisSecond.sumThenReset();
        long retrySkippedCapacity = retrySkippedCapacityThisSecond.sumThenReset();

        actualKafkaReceivedTotal.add(actualKafkaReceived);
        actualKafkaAcceptedTotal.add(actualKafkaAccepted);
        actualSendEventsTotal.add(actualSendEvents);
        actualAcksTotal.add(actualAcks);
        actualFailuresTotal.add(actualFailures);  // FIX: Accumulate failures properly
        dbRowsUpdatedTotal.add(dbRowsUpdated);
        retryEventsDispatchedTotal.add(retryDispatched);

        // Time elapsed since first event (in seconds)
        long elapsedMs = System.currentTimeMillis() - firstEventTime;
        long elapsedSeconds = elapsedMs / 1000;

        // === SYNTHETIC CALCULATION: Use shared RampupConfig for calculations ===
        double syntheticEmissionRate = rampupConfig.getCurrentRate(elapsedMs);
        long syntheticCumulativeSent = rampupConfig.getCumulativeEvents(elapsedMs);

        // 3. Calculate ACTUAL latency percentiles and stats
        double actualMeanLatency = intervalHistogram.getMean();
        long actualP50Latency = intervalHistogram.getValueAtPercentile(50.0);
        long actualP95Latency = intervalHistogram.getValueAtPercentile(95.0);
        long actualP99Latency = intervalHistogram.getValueAtPercentile(99.0);
        long actualP999Latency = intervalHistogram.getValueAtPercentile(99.9);
        long actualMaxLatency = intervalHistogram.getMaxValue();

        // Get ACTUAL cumulative counts
        long actualCumulativeKafkaReceived = actualKafkaReceivedTotal.sum();
        long actualCumulativeKafkaAccepted = actualKafkaAcceptedTotal.sum();
        long actualCumulativeSendEvents = actualSendEventsTotal.sum();
        long actualCumulativeAcks = actualAcksTotal.sum();
        long actualCumulativeFailures = actualFailuresTotal.sum();  // FIX: Use the proper cumulative counter
        long actualCumulativeDbRowsUpdated = dbRowsUpdatedTotal.sum();
        long actualCumulativeRetryDispatched = retryEventsDispatchedTotal.sum();

        // Query DB for pending deliveries count (how healthy we are)
        long actualPendingDeliveries = deliveryStatusRepository.countPending();

        // Calculate lag: how far behind expected are we?
        long actualLagBehind = syntheticCumulativeSent - actualCumulativeAcks;

        // Calculate ACTUAL success rate
        double actualSuccessRate = actualSendAttempts > 0
            ? (actualAcks * 100.0 / actualSendAttempts)
            : 100.0;

        // 4. Write CSV with time-series data only (constants are in header comments)
        csvWriter.printf("%d,%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.2f,%.2f,%d,%d,%d,%d,%d%n",
                elapsedSeconds,                 // TimeSeconds
                syntheticEmissionRate,          // Expected emission rate (calculated from config)
                actualKafkaReceived,            // ACTUAL: Events received from Kafka this second
                actualKafkaAccepted,            // ACTUAL: Events persisted/accepted this second
                actualSendEvents,               // ACTUAL: Events included in outbound sends this second
                actualAcks,                     // ACTUAL: Measured ACKs this second
                actualSendAttempts,             // ACTUAL: Measured send attempts this second
                actualFailures,                 // ACTUAL: Measured failures this second
                dbBatches,                      // ACTUAL: DB batches executed this second
                dbRowsUpdated,                  // ACTUAL: Rows updated this second
                dbTimeMs,                       // ACTUAL: DB time spent this second (ms)
                retryCycles,                    // ACTUAL: Retry cycles executed this second
                retryEvents,                    // ACTUAL: Pending events considered for retry this second
                retryDispatched,                // ACTUAL: Events actually re-dispatched this second
                retrySkippedCapacity,           // ACTUAL: Retry cycles skipped due to capacity
                actualCumulativeKafkaReceived,  // ACTUAL: Total events received from Kafka
                actualCumulativeKafkaAccepted,  // ACTUAL: Total events persisted/accepted
                actualCumulativeSendEvents,     // ACTUAL: Total events sent outbound
                actualCumulativeAcks,           // ACTUAL: Total ACKs received
                actualCumulativeDbRowsUpdated,  // ACTUAL: Total rows updated in DB
                actualCumulativeRetryDispatched,// ACTUAL: Total events re-dispatched via retry
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

