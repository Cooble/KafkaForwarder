package com.example.forwarder.websocket;

import com.example.common.AckMessage;
import com.example.common.DataMessage;
import com.example.common.ExternalData;
import com.example.common.WebSocketMessage;
import com.example.forwarder.model.ExternalDataTableEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dedicated sender for a single WebSocket session.
 * Runs in its own thread to handle the single-threaded nature of WebSocket sending.
 * Multiple Kafka/DB threads can safely queue messages, which are sent sequentially.
 */
public class WebSocketSessionSender implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WebSocketSessionSender.class);

    private final Long clientId;
    private final String clientIdentifier;
    private final WebSocketSession session;
    private final ObjectMapper objectMapper;
    private final long ackTimeoutMs;

    // Dedicated queue for this session - multiple threads can safely add to it
    private final BlockingQueue<SendTask> sendQueue;
    private static final int MAX_QUEUE_SIZE = 10000;

    // Pending ACKs for this session
    private final Map<String, CompletableFuture<Boolean>> pendingAcks = new ConcurrentHashMap<>();

    // Timeout executor for this session
    private final ScheduledExecutorService timeoutExecutor;

    // Thread running this sender
    private final Thread senderThread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public WebSocketSessionSender(Long clientId, String clientIdentifier, WebSocketSession session,
                                   ObjectMapper objectMapper, long ackTimeoutMs) {
        this.clientId = clientId;
        this.clientIdentifier = clientIdentifier;
        this.session = session;
        this.objectMapper = objectMapper;
        this.ackTimeoutMs = ackTimeoutMs;
        this.sendQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
        this.timeoutExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-timeout-client-" + clientId);
            t.setDaemon(true);
            return t;
        });
        this.senderThread = new Thread(this, "ws-sender-client-" + clientId);
        this.senderThread.setDaemon(true);
        this.senderThread.start();

        log.info("WebSocket sender started for client {} ({})", clientId, clientIdentifier);
    }

    /**
     * Queue a batch of data to be sent. Returns immediately (non-blocking).
     * If queue is full, drops the message and returns failed future.
     */
    public CompletableFuture<Boolean> queueBatch(List<ExternalDataTableEntry> dataList) {
        if (!running.get() || !session.isOpen()) {
            return CompletableFuture.completedFuture(false);
        }


        List<Long> dataIds = dataList.stream().map(ExternalDataTableEntry::getId).toList();
        List<ExternalData> externalDataList = dataList.stream()
                .map(data -> new ExternalData( data.getDocumentId(), data.getCustomerId(),
                        data.getCurrency(), data.getTotalCents(), data.getPayloadJson()))
                .toList();

        DataMessage payload = new DataMessage(dataIds, externalDataList);
        String messageId = generateBatchMessageId(dataIds);
        WebSocketMessage wsMessage = new WebSocketMessage(WebSocketMessage.MessageType.DATA, payload);

        CompletableFuture<Boolean> ackFuture = new CompletableFuture<>();
        SendTask task = new SendTask(wsMessage, messageId, dataIds, ackFuture);

        // Try to add to queue - if full, reject immediately
        if (sendQueue.offer(task)) {
            return ackFuture;
        } else {
            log.warn("Send queue full for client {} - dropping batch of {}", clientId, dataIds.size());
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * Main sending loop - runs in dedicated thread.
     * Processes queue sequentially, respecting WebSocket's single-threaded nature.
     */
    @Override
    public void run() {
        log.info("WebSocket sender thread started for client {}", clientId);

        while (running.get()) {
            try {
                // Block until message available (or 1 second timeout)
                SendTask task = sendQueue.poll(1, TimeUnit.SECONDS);
                if (task == null) {
                    continue;
                }

                // Check session is still open
                if (!session.isOpen()) {
                    log.warn("Session closed for client {} - failing queued message", clientId);
                    task.ackFuture.complete(false);
                    continue;
                }

                // Send the message
                try {
                    String jsonMessage = objectMapper.writeValueAsString(task.wsMessage);

                    // This is the ONLY place we call session.sendMessage for this session
                    // No need for locks - only this thread sends
                    session.sendMessage(new TextMessage(jsonMessage));

                    // Register for ACK
                    pendingAcks.put(task.messageId, task.ackFuture);

                    // Schedule timeout
                    timeoutExecutor.schedule(() -> {
                        CompletableFuture<Boolean> future = pendingAcks.remove(task.messageId);
                        if (future != null && !future.isDone()) {
                            future.complete(false);
                            log.debug("ACK timeout for message {} to client {}", task.messageId, clientId);
                        }
                    }, ackTimeoutMs, TimeUnit.MILLISECONDS);

                } catch (Exception e) {
                    log.error("Failed to send WebSocket message to client {}: {}", clientId, e.getMessage());
                    task.ackFuture.complete(false);
                }

            } catch (InterruptedException e) {
                log.info("WebSocket sender interrupted for client {}", clientId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in WebSocket sender for client {}: {}", clientId, e.getMessage(), e);
            }
        }

        log.info("WebSocket sender thread stopped for client {}", clientId);
    }

    /**
     * Handle ACK from client - called from WebSocket receive thread.
     */
    public void handleAck(AckMessage ackMessage) {
        // Try both individual and batch message IDs
        for (Long dataId : ackMessage.dataIds()) {
            String messageId = generateMessageId(dataId);
            CompletableFuture<Boolean> ackFuture = pendingAcks.remove(messageId);
            if (ackFuture != null) {
                ackFuture.complete(true);
            }
        }

        String batchMessageId = generateBatchMessageId(ackMessage.dataIds());
        CompletableFuture<Boolean> batchAckFuture = pendingAcks.remove(batchMessageId);
        if (batchAckFuture != null) {
            batchAckFuture.complete(true);
        }
    }

    /**
     * Get current queue size.
     */
    public int getQueueSize() {
        return sendQueue.size();
    }

    /**
     * Check if queue has capacity.
     */
    public boolean hasCapacity() {
        return sendQueue.remainingCapacity() > 0;
    }

    /**
     * Get available capacity.
     */
    public int getAvailableCapacity() {
        return sendQueue.remainingCapacity();
    }

    /**
     * Stop this sender gracefully.
     */
    public void shutdown() {
        log.info("Shutting down WebSocket sender for client {} - queue size: {}", clientId, sendQueue.size());
        running.set(false);

        // Fail all pending ACKs
        pendingAcks.values().forEach(future -> future.complete(false));
        pendingAcks.clear();

        // Shutdown timeout executor
        timeoutExecutor.shutdown();

        // Interrupt sender thread
        senderThread.interrupt();

        try {
            senderThread.join(5000);
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for sender thread to stop");
            Thread.currentThread().interrupt();
        }

        log.info("WebSocket sender shutdown complete for client {}", clientId);
    }

    private String generateMessageId(Long dataId) {
        return "msg_" + dataId + "_" + clientId;
    }

    private String generateBatchMessageId(List<Long> dataIds) {
        return "batch_" + dataIds.hashCode() + "_" + clientId;
    }

    /**
     * Represents a message to be sent.
     */
    private record SendTask(
        WebSocketMessage wsMessage,
        String messageId,
        List<Long> dataIds,
        CompletableFuture<Boolean> ackFuture
    ) {}
}

