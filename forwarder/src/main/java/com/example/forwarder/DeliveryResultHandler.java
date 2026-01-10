package com.example.forwarder;

import com.example.forwarder.db.DbService;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.pipeline.ForwarderExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Handles the result of sending data to clients.
 * Centralizes the logic for marking deliveries as confirmed.
 * Uses lock-free async queuing to decouple HTTP threads from DB operations.
 *
 * Now uses ForwarderExecutorService instead of its own thread pool.
 */
@Service
public class DeliveryResultHandler {
    private static final Logger log = LoggerFactory.getLogger(DeliveryResultHandler.class);

    @Autowired
    private DbService dbService;

    @Autowired
    private MetricsService metricsService;

    @Autowired
    private ForwarderExecutorService executorService;

    // Lock-free queue to decouple HTTP callbacks from DB updates
    private final ConcurrentLinkedQueue<DeliveryUpdate> updateQueue = new ConcurrentLinkedQueue<>();
    private final LongAdder queueSize = new LongAdder();
    private final LongAdder droppedUpdates = new LongAdder();
    private static final int MAX_QUEUE_SIZE = 20000; // Soft limit


    // Track active async tasks to prevent overwhelming the DB
    private final LongAdder activeBatchTasks = new LongAdder();
    private static final int MAX_CONCURRENT_BATCHES = 8;

    /**
     * Represents a pending delivery update to be processed asynchronously.
     */
    public record DeliveryUpdate(Long dataId, Long clientId, long bornTimeMs, boolean success) {}

    /**
     * Handles successful or failed delivery attempt.
     * Queues the update for async processing - does NOT block HTTP thread.
     * Lock-free operation for maximum throughput.
     */
    public void handleSendResult(MessageSender.SendResult result, DeliveryStatus status, String clientIdentifier) {
        if (result.success()) {
            // Check soft limit before adding (prevents unbounded growth)
            long currentSize = queueSize.sum();
            if (currentSize > MAX_QUEUE_SIZE) {
                droppedUpdates.increment();
                log.error("Update queue overflow! Size={}, Dropped data {} client {}",
                    currentSize, result.dataId(), result.clientId());
                return;
            }

            // Lock-free add - no contention between HTTP threads
            updateQueue.add(new DeliveryUpdate(result.dataId(), result.clientId(), status.getBornTimeMs(), true));
            queueSize.increment();
        }
        // Leave as pending - ResendService will retry later
    }

    /**
     * Handles WebSocket ACK - queues update for async DB processing.
     * Called when client sends ACK via WebSocket.
     * Uses born time of 0 since we don't track it for WebSocket ACKs.
     */
    public void handleWebSocketAck(Long dataId, Long clientId) {
        // Check soft limit before adding (prevents unbounded growth)
        long currentSize = queueSize.sum();
        if (currentSize > MAX_QUEUE_SIZE) {
            droppedUpdates.increment();
            log.error("Update queue overflow! Size={}, Dropped WebSocket ACK for data {} client {}",
                currentSize, dataId, clientId);
            return;
        }

        // Lock-free add - no contention between WebSocket receive threads
        updateQueue.add(new DeliveryUpdate(dataId, clientId, 0, true));
        queueSize.increment();
    }

    /**
     * Handles exception during send attempt.
     * Leaves delivery as pending for retry.
     */
    public void handleSendException(Long dataId, String clientIdentifier, DeliveryStatus status, Throwable ex) {
        log.debug("Exception sending data {} to client {}: {}", dataId, clientIdentifier, ex.getMessage());
        // Leave as pending - ResendService will retry later
    }

    /**
     * Processes queued delivery updates in batches.
     * Runs every 50ms for aggressive queue draining at high throughput.
     * Uses async processing to prevent blocking the scheduler thread.
     */
    @Scheduled(fixedRate = 50)
    public void processUpdateQueue() {
        // Skip if we have too many concurrent batch tasks - prevents DB overload
        if (activeBatchTasks.sum() >= MAX_CONCURRENT_BATCHES) {
            log.warn("Skipping batch drain - {} active tasks already running", activeBatchTasks.sum());
            return;
        }

        List<DeliveryUpdate> batch = new ArrayList<>(2000);
        // Drain up to 2000 updates from lock-free queue
        DeliveryUpdate update;
        int drained = 0;

        while (drained < 2000 && (update = updateQueue.poll()) != null) {
            batch.add(update);
            drained++;
        }

        if (batch.isEmpty()) {
            return;
        }

        // Update queue size counter
        queueSize.add(-drained);

        // Process asynchronously - DON'T block scheduler thread!
        final List<DeliveryUpdate> toProcess = batch;
        final int batchSize = toProcess.size();

        activeBatchTasks.increment();

        executorService.submitAckTask(() -> {
            long startTime = System.currentTimeMillis();
            try {
                int processed = dbService.markAsConfirmedBatch(toProcess);

                // Record metrics for each ACK - track time from Kafka arrival to ACK
                for (DeliveryUpdate deliveryUpdate : toProcess) {
                    if (deliveryUpdate.success() && deliveryUpdate.bornTimeMs() > 0) {
                        metricsService.recordAck(deliveryUpdate.bornTimeMs());
                    }
                }

                long duration = System.currentTimeMillis() - startTime;
                if (duration > 100) {
                    log.warn("Slow batch processing: {} updates in {}ms ({} updates/sec)",
                        processed, duration, (int)(processed * 1000.0 / duration));
                }
            } catch (Exception e) {
                log.error("Failed to process batch of {} updates: {}", batchSize, e.getMessage(), e);
            } finally {
                activeBatchTasks.decrement();
            }
        });
    }

      /**
     * Handles a batch send result for multiple delivery statuses at once.
     * This is more efficient than per-event handling.
     */
    public void handleBatchSendResult(MessageSender.BatchSendResult batchResult, List<DeliveryStatus> statuses, String clientIdentifier) {
        if (batchResult.success()) {
            long currentSize = queueSize.sum();
            if (currentSize + statuses.size() > MAX_QUEUE_SIZE) {
                droppedUpdates.add(statuses.size());
                log.error("Update queue overflow! Size={}, Dropped {} data for client {}", currentSize, statuses.size(), clientIdentifier);
                return;
            }
            for (int i = 0; i < statuses.size(); i++) {
                DeliveryStatus status = statuses.get(i);
                updateQueue.add(new DeliveryUpdate(batchResult.dataIds().get(i), batchResult.clientId(), status.getBornTimeMs(), true));
            }
            queueSize.add(statuses.size());
        }
        // If failed, leave as pending for retry
    }

    public long getQueueSize() {
        return queueSize.sum();
    }

    public long getDroppedUpdates() {
        return droppedUpdates.sum();
    }
}
