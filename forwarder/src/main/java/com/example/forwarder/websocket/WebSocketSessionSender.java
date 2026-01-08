package com.example.forwarder.websocket;

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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dedicated sender for a single WebSocket session.
 * Runs in its own thread to handle the single-threaded nature of WebSocket sending.
 * Multiple Kafka/DB threads can safely queue messages, which are sent sequentially.
 *
 * Simple approach: just send messages, don't track ACKs with futures.
 * ACK handling is done directly via DeliveryResultHandler to update DB.
 * ResendService handles retries for events that stay PENDING.
 */
public class WebSocketSessionSender implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WebSocketSessionSender.class);

    private final Long clientId;
    private final String clientIdentifier;
    private final WebSocketSession session;
    private final ObjectMapper objectMapper;

    // Dedicated queue for this session - multiple threads can safely add to it
    private final BlockingQueue<SendTask> sendQueue;
    private static final int MAX_QUEUE_SIZE = 10000;

    // Thread running this sender
    private final Thread senderThread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public WebSocketSessionSender(Long clientId, String clientIdentifier, WebSocketSession session,
                                   ObjectMapper objectMapper) {
        this.clientId = clientId;
        this.clientIdentifier = clientIdentifier;
        this.session = session;
        this.objectMapper = objectMapper;
        this.sendQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
        this.senderThread = new Thread(this, "ws-sender-client-" + clientId);
        this.senderThread.setDaemon(true);
        this.senderThread.start();

        log.info("WebSocket sender started for client {} ({})", clientId, clientIdentifier);
    }

    /**
     * Queue a batch of data to be sent. Returns immediately (non-blocking).
     * If queue is full, drops the message and returns failed future.
     * The future completes immediately - ACK tracking is handled separately via DB.
     */
    public CompletableFuture<Boolean> queueBatch(List<ExternalDataTableEntry> dataList) {
        if (!running.get() || !session.isOpen()) {
            return CompletableFuture.completedFuture(false);
        }

        List<Long> dataIds = dataList.stream().map(ExternalDataTableEntry::getId).toList();
        List<ExternalData> externalDataList = dataList.stream()
                .map(data -> new ExternalData(data.getDocumentId(), data.getCustomerId(),
                        data.getCurrency(), data.getTotalCents(), data.getPayloadJson()))
                .toList();

        DataMessage payload = new DataMessage(dataIds, externalDataList);
        WebSocketMessage wsMessage = new WebSocketMessage(WebSocketMessage.MessageType.DATA, payload);

        SendTask task = new SendTask(wsMessage, dataIds);

        // Try to add to queue - if full, reject immediately
        if (sendQueue.offer(task)) {
            // Return success immediately - we don't wait for ACK via futures
            // ACK will be handled separately and update DB directly
            return CompletableFuture.completedFuture(true);
        } else {
            log.warn("Send queue full for client {} - dropping batch of {}", clientId, dataIds.size());
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * Main sending loop - runs in dedicated thread.
     * Processes queue sequentially, respecting WebSocket's single-threaded nature.
     * Just sends messages - ACK tracking happens separately via DeliveryResultHandler.
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
                    log.warn("Session closed for client {} - discarding queued message", clientId);
                    continue;
                }

                // Send the message
                try {
                    String jsonMessage = objectMapper.writeValueAsString(task.wsMessage);
                    session.sendMessage(new TextMessage(jsonMessage));
                    //log.debug("Sent batch of {} events to client {}", task.dataIds.size(), clientId);

                    // No ACK tracking here - ACK will be handled by ForwarderWebSocketHandler
                    // which will update DB directly via DeliveryResultHandler

                } catch (Exception e) {
                    log.error("Failed to send WebSocket message to client {}: {}", clientId, e.getMessage());
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

    /**
     * Represents a message to be sent.
     */
    private record SendTask(
        WebSocketMessage wsMessage,
        List<Long> dataIds
    ) {}
}

