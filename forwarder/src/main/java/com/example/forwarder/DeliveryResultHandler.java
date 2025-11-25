package com.example.forwarder;

import com.example.forwarder.db.DbService;
import com.example.forwarder.model.DeliveryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Handles the result of sending data to clients.
 * Centralizes the logic for marking deliveries as confirmed or updating retry status.
 */
@Service
public class DeliveryResultHandler {
    private static final Logger log = LoggerFactory.getLogger(DeliveryResultHandler.class);

    @Autowired
    private DbService dbService;

    /**
     * Handles successful or failed delivery attempt.
     * On success: marks as confirmed and triggers cleanup.
     * On failure: updates attempt count and last attempt timestamp.
     */
    public void handleSendResult(SendService.SendResult result, DeliveryStatus status, String clientIdentifier) {
        if (result.success()) {
            log.info("Client {} confirmed receipt of data {} via HTTP 200",
                    clientIdentifier, result.dataId());
            // HTTP 200 = confirmed! Mark as confirmed immediately
            dbService.markAsConfirmedByClientId(result.dataId(), result.clientId());
        } else {
            log.warn("Failed to send data {} to client {} (attempt {}/max)",
                    result.dataId(), clientIdentifier, status.getAttemptCount() + 1);
            status.setLastAttempt(LocalDateTime.now());
            status.incrementAttemptCount();
            dbService.updateDeliveryStatus(status);
        }
    }

    /**
     * Handles exception during send attempt.
     * Updates attempt count and last attempt timestamp.
     */
    public void handleSendException(Long dataId, String clientIdentifier, DeliveryStatus status, Throwable ex) {
        log.error("Exception sending data {} to client {}: {}",
                dataId, clientIdentifier, ex.getMessage());
        status.setLastAttempt(LocalDateTime.now());
        status.incrementAttemptCount();
        dbService.updateDeliveryStatus(status);
    }
}

