package com.example.forwarder;

import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class ResendService {
    @Autowired
    private DbService dbService;
    @Autowired
    private SendService sendService;

    @Value("${forwarder.retry.interval.ms:5000}")
    private int retryIntervalMs;

    @Value("${forwarder.retry.max.attempts:5}")
    private int maxAttempts;

    private static final Logger log = LoggerFactory.getLogger(ResendService.class);

    @Scheduled(fixedDelayString = "${forwarder.retry.check.interval.ms:10000}")
    public void resendPendingDeliveries() {
        log.debug("Checking for pending deliveries to retry...");

        final List<DeliveryStatus> pendingDeliveries = dbService.getPendingDeliveries(retryIntervalMs / 1000, maxAttempts);

        if (!pendingDeliveries.isEmpty()) {
            log.info("Found {} pending deliveries to retry (ordered by sequence number)", pendingDeliveries.size());
        }

        for (final DeliveryStatus status : pendingDeliveries) {
            final Optional<ExternalDataTableEntry> dataOpt = dbService.getExternalDataById(status.getExternalDataId());
            final Optional<Client> clientOpt = dbService.getClientById(status.getClientId());

            if (dataOpt.isEmpty()) {
                log.warn("Data not found for delivery status {}. Skipping.", status.getId());
                continue;
            }
            
            if (clientOpt.isEmpty()) {
                log.warn("Client not found for delivery status {}. Skipping.", status.getId());
                continue;
            }

            final ExternalDataTableEntry data = dataOpt.get();
            final Client client = clientOpt.get();

            if (client.getSessionId() == null) {
                log.info("Client {} is disconnected, skipping resend", client.getClientIdentifier());
                continue;
            }

            log.info("Retrying delivery of data {} to client {} (attempt {}/{})", data.getId(), client.getClientIdentifier(), status.getAttemptCount() + 1, maxAttempts);

            // Update attempt info before sending
            status.setLastAttempt(LocalDateTime.now());
            status.incrementAttemptCount();
            dbService.updateDeliveryStatus(status);

            sendService.sendToClient(client.getClientIdentifier(), data);
        }

        // Cleanup failed deliveries that exceeded max attempts
        dbService.deleteFailedDeliveries(maxAttempts);
    }
}

