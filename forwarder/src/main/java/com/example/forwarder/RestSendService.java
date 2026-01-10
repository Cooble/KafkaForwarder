package com.example.forwarder;

import com.example.common.ExternalData;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

@Service
@ConditionalOnProperty(name = "forwarder.transport.mode", havingValue = "rest", matchIfMissing = true)
public class RestSendService implements MessageSender {
    private static final Logger log = LoggerFactory.getLogger(RestSendService.class);

    @Value("${forwarder.client.timeout.ms:10000}")
    private int timeoutMs;

    @Value("${forwarder.client.max.concurrent.requests:500}")
    private int maxConcurrentRequests;

    @Autowired
    private TransformationService transformationService;

    private final WebClient webClient;

    // Track active HTTP requests to prevent overwhelming the connection pool
    //todo maybe use some adder so that it will be parallel friendly? (for each thread separate counter)
    private final LongAdder activeRequests = new LongAdder();


    public RestSendService(WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Check if we have capacity for more HTTP requests
     */
    public boolean hasCapacity() {
        return activeRequests.sum() < maxConcurrentRequests;
    }

    /**
     * Get number of currently active HTTP requests
     */
    public int getActiveRequests() {
        return activeRequests.intValue();
    }

    /**
     * Get number of available request slots
     */
    public int getAvailableCapacity() {
        return maxConcurrentRequests - activeRequests.intValue();
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
            log.debug("HTTP capacity exhausted ({}/{} active). Rejecting batch of {} events to client {} - will retry later",
                    activeRequests.get(), maxConcurrentRequests, dataList.size(), client.getClientIdentifier());

            // Return failed result immediately - retry mechanism will handle it later
            return CompletableFuture.completedFuture(new BatchSendResult(false, dataIds, client.getId()));
        }

        activeRequests.increment();

        // Create payload - convert all data entries to ExternalData
        List<ExternalData> payload = dataList.stream()
                .map(data -> transformationService.transform(data))
                .toList();

        return webClient.post()
                .uri(client.getClientUrl() + "/data/batch")
                .bodyValue(payload)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofMillis(timeoutMs))
                .doFinally(signal -> activeRequests.decrement())  // Always decrement when done
                .map(response -> new BatchSendResult(true, dataIds, client.getId()))
                .onErrorResume(error -> Mono.just(new BatchSendResult(false, dataIds, client.getId())))
                .toFuture();
    }
}
