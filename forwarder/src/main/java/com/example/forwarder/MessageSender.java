package com.example.forwarder;

import com.example.forwarder.model.Client;
import com.example.forwarder.model.ExternalDataTableEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for sending messages to clients.
 * Implementations: REST (HTTP) and WebSocket
 */
public interface MessageSender {

    CompletableFuture<SendResult> sendToClient(ExternalDataTableEntry data, Client client);

    CompletableFuture<BatchSendResult> sendBatchToClient(List<ExternalDataTableEntry> dataList, Client client);

    boolean hasCapacity();

    int getActiveRequests();

    int getAvailableCapacity();

    record SendResult(boolean success, Long dataId, Long clientId) {}

    record BatchSendResult(boolean success, List<Long> dataIds, Long clientId) {}
}

