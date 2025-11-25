package com.example.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

@Service
public class SendService {
    private static final Logger log = LoggerFactory.getLogger(SendService.class);

    @Value("${forwarder.url}")
    private String forwarderUrl;

    @Value("${client.identifier:unknown}")
    private String clientIdentifier;

    private final WebClient webClient;

    public SendService(WebClient webClient) {
        this.webClient = webClient;
    }



    public void sendConfirmation(Long dataId, String clientIp) {
        String identifier = !clientIdentifier.equals("unknown") ? clientIdentifier : clientIp;

        log.info("Sending confirmation for data ID {} to forwarder as client {}", dataId, identifier);

        ConfirmationRequest confirmationRequest = new ConfirmationRequest(dataId, identifier);

        webClient.post()
                .uri(forwarderUrl + "/confirm")
                .bodyValue(confirmationRequest)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofSeconds(5))
                .subscribe(
                    response -> log.info("Successfully confirmed data ID {}", dataId),
                    error -> log.error("Failed to send confirmation for data ID {}: {}", dataId, error.getMessage())
                );
    }

    public record ConfirmationRequest(Long dataId, String clientIdentifier) {}
}

