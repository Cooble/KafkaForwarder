package com.example.forwarder.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Centralized executor service for all forwarder async operations.
 * Consolidates the previously scattered thread pools into one managed component.
 *
 * Thread groups:
 * - pipeline: Main processing (transform → persist → dispatch)
 * - ack: ACK/result batch processing for DB updates
 * - cleanup: Low-priority cleanup tasks
 */
@Component
public class ForwarderExecutorService {
    private static final Logger log = LoggerFactory.getLogger(ForwarderExecutorService.class);

    private final ExecutorService pipelineExecutor;
    private final ExecutorService ackExecutor;
    private final ExecutorService cleanupExecutor;

    private final LongAdder pendingPipelineTasks = new LongAdder();
    private final LongAdder pendingAckTasks = new LongAdder();

    public ForwarderExecutorService() {
        // Pipeline executor: handles main Kafka → DB → dispatch flow (was dbExecutor in KafkaService)
        this.pipelineExecutor = Executors.newFixedThreadPool(10, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "pipeline-worker-" + counter.incrementAndGet());
                t.setDaemon(false);
                return t;
            }
        });

        // ACK executor: handles delivery result processing (was batchExecutor in DeliveryResultHandler)
        this.ackExecutor = Executors.newFixedThreadPool(4, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "ack-worker-" + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });

        // Cleanup executor: low-priority background tasks (was cleanupExecutor in DbStatsService)
        this.cleanupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "cleanup-worker");
            t.setDaemon(true);
            return t;
        });

        log.info("ForwarderExecutorService initialized: pipeline=10, ack=4, cleanup=1 threads");
    }

    /**
     * Submit a pipeline task (Kafka batch processing).
     */
    public CompletableFuture<Void> submitPipelineTask(Runnable task) {
        pendingPipelineTasks.increment();
        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                pendingPipelineTasks.decrement();
            }
        }, pipelineExecutor);
    }

    /**
     * Submit an ACK processing task.
     */
    public CompletableFuture<Void> submitAckTask(Runnable task) {
        pendingAckTasks.increment();
        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                pendingAckTasks.decrement();
            }
        }, ackExecutor);
    }

    /**
     * Submit a cleanup task (fire-and-forget).
     */
    public void submitCleanupTask(Runnable task) {
        cleanupExecutor.execute(task);
    }

    /**
     * Get the pipeline executor for direct use when needed.
     */
    public ExecutorService getPipelineExecutor() {
        return pipelineExecutor;
    }

    /**
     * Get the ACK executor for direct use when needed.
     */
    public ExecutorService getAckExecutor() {
        return ackExecutor;
    }

    public long getPendingPipelineTasks() {
        return pendingPipelineTasks.sum();
    }

    public long getPendingAckTasks() {
        return pendingAckTasks.sum();
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down ForwarderExecutorService...");
        log.info("Pending tasks - pipeline: {}, ack: {}", pendingPipelineTasks.sum(), pendingAckTasks.sum());

        // Shutdown in order: pipeline first (stop accepting new work), then ack, then cleanup
        shutdownExecutor("pipeline", pipelineExecutor, 60);
        shutdownExecutor("ack", ackExecutor, 10);
        shutdownExecutor("cleanup", cleanupExecutor, 5);

        log.info("ForwarderExecutorService shutdown complete");
    }

    private void shutdownExecutor(String name, ExecutorService executor, int timeoutSeconds) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                log.warn("{} executor did not terminate in {}s, forcing shutdown", name, timeoutSeconds);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down {} executor", name);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

