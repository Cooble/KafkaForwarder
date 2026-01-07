package com.example.forwarder;

import com.example.common.ExternalData;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class SendService {
    private static final Logger log = LoggerFactory.getLogger(SendService.class);

    @Value("${forwarder.client.timeout.ms:10000}")
    private int timeoutMs;

    @Value("${forwarder.client.max.concurrent.requests:500}")
    private int maxConcurrentRequests;

    private final WebClient webClient;

    // Track active HTTP requests to prevent overwhelming the connection pool
    private final AtomicInteger activeRequests = new AtomicInteger(0);

    private final AtomicLong sentCount = new AtomicLong(0);
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failCount = new AtomicLong(0);
    private final AtomicLong rejectedCount = new AtomicLong(0);

    public SendService(WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Check if we have capacity for more HTTP requests
     */
    public boolean hasCapacity() {
        return activeRequests.get() < maxConcurrentRequests;
    }

    /**
     * Get number of currently active HTTP requests
     */
    public int getActiveRequests() {
        return activeRequests.get();
    }

    /**
     * Get number of available request slots
     */
    public int getAvailableCapacity() {
        return maxConcurrentRequests - activeRequests.get();
    }

    public CompletableFuture<SendResult> sendToClient(ExternalDataTableEntry data, Client client) {
        sentCount.incrementAndGet();

        // Create payload - just send the data, no need for ID wrapper
        ExternalData payload = new ExternalData(
            data.getMsg(),
            data.getName(),
            data.getExternalNew()
        );

        return webClient.post()
                .uri(client.getClientUrl() + "/data")
                .bodyValue(payload)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofMillis(timeoutMs))
                .map(response -> {
                    successCount.incrementAndGet();
                    return new SendResult(true, data.getId(), client.getId());
                })
                .onErrorResume(error -> {
                    failCount.incrementAndGet();
                    return Mono.just(new SendResult(false, data.getId(), client.getId()));
                })
                .toFuture();
    }

    /**
     * Send a batch of events to a client in a single HTTP request (much more efficient!)
     * Returns failed future immediately if no HTTP capacity available.
     */
    public CompletableFuture<BatchSendResult> sendBatchToClient(List<ExternalDataTableEntry> dataList, Client client) {
        // Extract data IDs for result tracking
        List<Long> dataIds = dataList.stream()
                .map(ExternalDataTableEntry::getId)
                .toList();

        // Check if we have capacity - if not, fail immediately (don't block, don't queue)
        if (!hasCapacity()) {
            failCount.addAndGet(dataList.size());
            rejectedCount.addAndGet(dataList.size());
            log.debug("HTTP capacity exhausted ({}/{} active). Rejecting batch of {} events to client {} - will retry later",
                    activeRequests.get(), maxConcurrentRequests, dataList.size(), client.getClientIdentifier());

            // Return failed result immediately - retry mechanism will handle it later
            return CompletableFuture.completedFuture(new BatchSendResult(false, dataIds, client.getId()));
        }

        sentCount.addAndGet(dataList.size());
        activeRequests.incrementAndGet();

        // Create payload - convert all data entries to ExternalData
        List<ExternalData> payload = dataList.stream()
                .map(data -> new ExternalData(data.getMsg(), data.getName(), data.getExternalNew()))
                .toList();

        return webClient.post()
                .uri(client.getClientUrl() + "/data/batch")
                .bodyValue(payload)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofMillis(timeoutMs))
                .doFinally(signal -> activeRequests.decrementAndGet())  // Always decrement when done
                .map(response -> {
                    successCount.addAndGet(dataList.size());
                    return new BatchSendResult(true, dataIds, client.getId());
                })
                .onErrorResume(error -> {
                    failCount.addAndGet(dataList.size());
                    return Mono.just(new BatchSendResult(false, dataIds, client.getId()));
                })
                .toFuture();
    }

    public record SendResult(boolean success, Long dataId, Long clientId) {}
    public record BatchSendResult(boolean success, List<Long> dataIds, Long clientId) {}

    @Scheduled(fixedRate = 1000)
    public void logStats() {
        long sent = sentCount.getAndSet(0);
        long success = successCount.getAndSet(0);
        long fail = failCount.getAndSet(0);
        long rejected = rejectedCount.getAndSet(0);
        int active = activeRequests.get();

        if (sent > 0 || rejected > 0 || active > 0) {
            log.info("Send: {} sent/sec ({} success, {} failed, {} rejected due to capacity), {} active requests",
                    sent, success, fail, rejected, active);
        }
    }
}
