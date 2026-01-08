package com.example.forwarder;

import com.example.forwarder.db.DbService;
import com.example.forwarder.model.DeliveryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles the result of sending data to clients.
 * Centralizes the logic for marking deliveries as confirmed.
 * Uses lock-free async queuing to decouple HTTP threads from DB operations.
 */
@Service
public class DeliveryResultHandler {
    private static final Logger log = LoggerFactory.getLogger(DeliveryResultHandler.class);

    @Autowired
    private DbService dbService;


    // Lock-free queue to decouple HTTP callbacks from DB updates
    private final ConcurrentLinkedQueue<DeliveryUpdate> updateQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong queueSize = new AtomicLong(0);
    private final AtomicLong droppedUpdates = new AtomicLong(0);
    private static final int MAX_QUEUE_SIZE = 20000; // Soft limit

    // Dedicated thread pool for async DB batch processing
    // Multiple threads to handle concurrent batch writes without blocking scheduler
    private final ExecutorService batchExecutor = Executors.newFixedThreadPool(4, new ThreadFactory() {
        private final AtomicLong threadId = new AtomicLong(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "db-batch-processor-" + threadId.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    });

    // Track active async tasks to prevent overwhelming the DB
    private final AtomicLong activeBatchTasks = new AtomicLong(0);
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
            long currentSize = queueSize.get();
            if (currentSize > MAX_QUEUE_SIZE) {
                droppedUpdates.incrementAndGet();
                log.error("Update queue overflow! Size={}, Dropped data {} client {}",
                    currentSize, result.dataId(), result.clientId());
                return;
            }

            // Lock-free add - no contention between HTTP threads
            updateQueue.add(new DeliveryUpdate(result.dataId(), result.clientId(), status.getBornTimeMs(), true));
            queueSize.incrementAndGet();
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
        long currentSize = queueSize.get();
        if (currentSize > MAX_QUEUE_SIZE) {
            droppedUpdates.incrementAndGet();
            log.error("Update queue overflow! Size={}, Dropped WebSocket ACK for data {} client {}",
                currentSize, dataId, clientId);
            return;
        }

        // Lock-free add - no contention between WebSocket receive threads
        updateQueue.add(new DeliveryUpdate(dataId, clientId, 0, true));
        queueSize.incrementAndGet();
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
        if (activeBatchTasks.get() >= MAX_CONCURRENT_BATCHES) {
            log.warn("Skipping batch drain - {} active tasks already running", activeBatchTasks.get());
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
        queueSize.addAndGet(-drained);

        // Process asynchronously - DON'T block scheduler thread!
        // This is the key improvement - scheduler can keep draining queue
        final List<DeliveryUpdate> toProcess = batch; // Final for lambda
        final int batchSize = toProcess.size();

        activeBatchTasks.incrementAndGet();

        CompletableFuture.runAsync(() -> {
            long startTime = System.currentTimeMillis();
            try {
                // This DB call now runs in background thread pool
                int processed = dbService.markAsConfirmedBatch(toProcess);

                long duration = System.currentTimeMillis() - startTime;
                if (duration > 100) {
                    log.warn("Slow batch processing: {} updates in {}ms ({} updates/sec)",
                        processed, duration, (int)(processed * 1000.0 / duration));
                }
            } catch (Exception e) {
                log.error("Failed to process batch of {} updates: {}", batchSize, e.getMessage(), e);
            } finally {
                activeBatchTasks.decrementAndGet();
            }
        }, batchExecutor);
    }


    /**
     * Gracefully shutdown the batch executor on application shutdown.
     * Waits for in-flight batches to complete before terminating.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down DeliveryResultHandler - {} active batch tasks", activeBatchTasks.get());
        batchExecutor.shutdown();
        try {
            if (!batchExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Batch executor did not terminate in time, forcing shutdown");
                batchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Interrupted during shutdown", e);
            batchExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("DeliveryResultHandler shutdown complete - queue size: {}", updateQueue.size());
    }
}

