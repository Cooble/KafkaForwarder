package com.example.forwarder.websocket;

import com.example.forwarder.MessageSender;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.*;

/**
 * WebSocket send service using dedicated sender threads per session.
 * Much simpler than REST - just queue messages to the appropriate sender.
 * Each sender thread handles its own queue and ACK management.
 */
@Service
@ConditionalOnProperty(name = "forwarder.transport.mode", havingValue = "websocket")
public class WebSocketSendService implements MessageSender {
    private static final Logger log = LoggerFactory.getLogger(WebSocketSendService.class);

    @Autowired
    private WebSocketSessionManager sessionManager;


    @Override
    public boolean hasCapacity() {
        // Always return true - capacity is managed per-session by queue depth
        // If a specific session's queue is full, that send will fail
        return true;
    }

    @Override
    public int getActiveRequests() {
        // Not meaningful for WebSocket - each session has its own queue
        return 0;
    }

    @Override
    public int getAvailableCapacity() {
        // Not meaningful for WebSocket - capacity is per-session
        return Integer.MAX_VALUE;
    }

    @Override
    public CompletableFuture<BatchSendResult> sendBatchToClient(List<ExternalDataTableEntry> dataList, Client client) {
        List<Long> dataIds = dataList.stream().map(ExternalDataTableEntry::getId).toList();

        // Get the dedicated sender for this client
        WebSocketSessionSender sender = sessionManager.getSender(client.getId());
        if (sender == null) {
            log.debug("No WebSocket session for client {}", client.getClientIdentifier());
            return CompletableFuture.completedFuture(new BatchSendResult(false, dataIds, client.getId()));
        }

        // Queue the batch - sender thread will handle actual sending
        // Note: We return success=false to keep deliveries pending until ACKed
        sender.queueBatch(dataList);
        return CompletableFuture.completedFuture(new BatchSendResult(false, dataIds, client.getId()));
    }
}
