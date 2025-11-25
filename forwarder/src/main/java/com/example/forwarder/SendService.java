package com.example.forwarder;

import com.example.common.ExternalData;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Service
public class SendService {
    private static final Logger log = LoggerFactory.getLogger(SendService.class);

    @Value("${forwarder.client.timeout.ms:10000}")
    private int timeoutMs;

    private final WebClient webClient;

    public SendService(WebClient webClient) {
        this.webClient = webClient;
    }

    public CompletableFuture<SendResult> sendToClient(ExternalDataTableEntry data, Client client) {
        log.info("Sending data {} to client {} at {}", data.getId(), client.getClientIdentifier(), client.getClientUrl());

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
                .map(response -> new SendResult(true, data.getId(), client.getId()))
                .onErrorResume(error -> Mono.just(new SendResult(false, data.getId(), client.getId())))
                .toFuture();
    }

    public record SendResult(boolean success, Long dataId, Long clientId) {}
}
