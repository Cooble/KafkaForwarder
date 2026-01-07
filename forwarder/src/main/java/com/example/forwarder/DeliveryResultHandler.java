package com.example.forwarder;

import com.example.forwarder.db.DbService;
import com.example.forwarder.model.DeliveryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles the result of sending data to clients.
 * Centralizes the logic for marking deliveries as confirmed.
 */
@Service
public class DeliveryResultHandler {
    private static final Logger log = LoggerFactory.getLogger(DeliveryResultHandler.class);

    @Autowired
    private DbService dbService;

    @Autowired
    private MetricsService metricsService;

    private final AtomicLong acksReceived = new AtomicLong(0);
    private final AtomicLong failedDeliveries = new AtomicLong(0);

    /**
     * Handles successful or failed delivery attempt.
     * On success: marks as confirmed and triggers cleanup.
     * On failure: leaves as pending for retry.
     */
    public void handleSendResult(SendService.SendResult result, DeliveryStatus status, String clientIdentifier) {
        if (result.success()) {
            // HTTP 200 = confirmed! Mark as confirmed immediately
            dbService.markAsConfirmedByClientId(result.dataId(), result.clientId());
            metricsService.recordAck(status.getBornTimeMs());
            acksReceived.incrementAndGet();
        } else {
            // Leave as pending - ResendService will retry later
            failedDeliveries.incrementAndGet();
        }
    }

    /**
     * Handles exception during send attempt.
     * Leaves delivery as pending for retry.
     */
    public void handleSendException(Long dataId, String clientIdentifier, DeliveryStatus status, Throwable ex) {
        log.debug("Exception sending data {} to client {}: {}", dataId, clientIdentifier, ex.getMessage());
        // Leave as pending - ResendService will retry later
        failedDeliveries.incrementAndGet();
    }

    @Scheduled(fixedRate = 1000)
    public void logStats() {
        long acks = acksReceived.getAndSet(0);
        long fails = failedDeliveries.getAndSet(0);
        if (acks > 0 || fails > 0) {
            log.info("Delivery: {} ACKs/sec, {} failed/sec", acks, fails);
        }
    }
}

