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

import java.util.*;

@Service
public class ResendService {
    @Autowired
    private DbService dbService;
    @Autowired
    private SendService sendService;
    @Autowired
    private DeliveryResultHandler deliveryResultHandler;

    @Value("${forwarder.client.max.batch.size:100}")
    private int maxHttpBatchSize;

    private static final Logger log = LoggerFactory.getLogger(ResendService.class);

    @Scheduled(fixedDelayString = "${forwarder.retry.check.interval.ms:10000}")
    public void resendPendingDeliveries() {
        log.debug("Checking for pending deliveries to retry...");

        // Check if we have HTTP capacity - if not, skip this retry cycle
        if (!sendService.hasCapacity()) {
            log.debug("No HTTP capacity available ({} active). Skipping retry cycle - will try again later",
                    sendService.getActiveRequests());
            return;
        }

        List<DeliveryStatus> pendingDeliveries = dbService.getPendingDeliveries();

        if (pendingDeliveries.isEmpty()) {
            return;
        }

        log.info("Found {} pending deliveries to retry (ordered by sequence number)", pendingDeliveries.size());

        // Group pending deliveries by client (same as KafkaService does)
        Map<Long, List<PendingDeliveryItem>> deliveriesByClient = new HashMap<>();

        for (DeliveryStatus status : pendingDeliveries) {
            Optional<ExternalDataTableEntry> dataOpt = dbService.getExternalDataById(status.getExternalDataId());
            Optional<Client> clientOpt = dbService.getClientById(status.getClientId());

            if (dataOpt.isEmpty() || clientOpt.isEmpty()) {
                continue; // the y were deleted in the meantime
            }

            ExternalDataTableEntry data = dataOpt.get();
            Client client = clientOpt.get();

            deliveriesByClient.computeIfAbsent(client.getId(), k -> new ArrayList<>())
                .add(new PendingDeliveryItem(data, status, client));
        }

        // Send batched HTTP requests per client (same pattern as KafkaService)
        for (var entry : deliveriesByClient.entrySet()) {
            List<PendingDeliveryItem> items = entry.getValue();
            if (items.isEmpty()) continue;

            Client client = items.get(0).client;

            // Split into chunks to avoid huge payloads (same as KafkaService)
            for (int i = 0; i < items.size(); i += maxHttpBatchSize) {
                // Check capacity before each batch - stop if exhausted
                if (!sendService.hasCapacity()) {
                    log.debug("HTTP capacity exhausted during retry. Stopping - remaining will retry next cycle");
                    return;  // Exit completely - let next retry cycle handle remaining
                }

                int end = Math.min(i + maxHttpBatchSize, items.size());
                List<PendingDeliveryItem> chunk = items.subList(i, end);

                List<ExternalDataTableEntry> dataList = chunk.stream()
                    .map(item -> item.data)
                    .toList();
                List<DeliveryStatus> statusList = chunk.stream()
                    .map(item -> item.status)
                    .toList();

                sendBatchToClientAsync(dataList, client, statusList);
            }
        }
    }

    private void sendBatchToClientAsync(List<ExternalDataTableEntry> dataList, Client client, List<DeliveryStatus> statusList) {
        sendService.sendBatchToClient(dataList, client).thenAccept(result -> {
            // Handle result for all events in the batch
            for (int i = 0; i < statusList.size(); i++) {
                DeliveryStatus status = statusList.get(i);
                Long dataId = dataList.get(i).getId();
                // Create individual SendResult for each event in the batch
                SendService.SendResult individualResult = new SendService.SendResult(result.success(), dataId, result.clientId());
                deliveryResultHandler.handleSendResult(individualResult, status, client.getClientIdentifier());
            }
        }).exceptionally(ex -> {
            // Handle exception for all events in the batch
            for (int i = 0; i < statusList.size(); i++) {
                deliveryResultHandler.handleSendException(dataList.get(i).getId(), client.getClientIdentifier(), statusList.get(i), ex);
            }
            return null;
        });
    }

    // Helper record to hold pending delivery data
    private record PendingDeliveryItem(ExternalDataTableEntry data, DeliveryStatus status, Client client) {}
}

