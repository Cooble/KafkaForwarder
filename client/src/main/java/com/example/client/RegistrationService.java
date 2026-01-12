package com.example.client;

import com.example.common.RegistrationRequest;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

@Service
public class RegistrationService {
    private static final Logger log = LoggerFactory.getLogger(RegistrationService.class);

    @Value("${forwarder.url}")
    private String forwarderUrl;

    @Value("${client.url}")
    private String clientUrl;

    @Value("${client.subscribed.topics}")
    private String subscribedTopicsStr;

    @Value("${client.auto.register:true}")
    private boolean autoRegister;

    @Value("${client.registration.retry.max.attempts:5}")
    private int maxRetryAttempts;

    @Value("${client.registration.retry.delay.ms:2000}")
    private long retryDelayMs;

    @Value("${client.identifier:#{null}}")
    private String clientIdentifier;

    @Autowired
    private WebClient webClient;

    @PostConstruct
    public void registerOnStartup() {
        if (!autoRegister) {
            log.info(" Auto-registration is disabled. Skipping registration with forwarder.");

            return;
        }

        List<String> topics = Arrays.asList(subscribedTopicsStr.split(","));
        log.info("Registering client with forwarder for topics: {}", topics);

        String identifier = clientIdentifier != null ? clientIdentifier : clientUrl;
        RegistrationRequest request = new RegistrationRequest(identifier, clientUrl, topics);

        // Retry registration with exponential backoff
        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            attempt++;
            try {
                webClient.post()
                        .uri(forwarderUrl + "/registration")
                        .bodyValue(request)
                        .retrieve()
                        .toBodilessEntity()
                        .timeout(Duration.ofSeconds(10))
                        .block();

                log.info("Successfully registered with forwarder on attempt {}", attempt);
                return; // Success! Exit the method

            } catch (Exception e) {

                long delay = retryDelayMs * attempt; // Linear backoff
                log.warn("Failed to register with forwarder (attempt {}/{}): {}. Retrying in {}ms...",
                        attempt, maxRetryAttempts, e.getMessage(), delay);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("Registration retry interrupted");
                    return;
                }

            }
        }
    }
}
