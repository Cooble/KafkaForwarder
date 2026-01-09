package com.example.forwarder;

import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.model.ExternalDataTableEntry;
import com.example.forwarder.pipeline.ForwarderPipeline;
import com.example.forwarder.pipeline.ForwarderPipeline.ClientBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Retries pending deliveries by reusing the pipeline's dispatch stage.
 * No longer duplicates send logic - delegates to ForwarderPipeline.
 */
@Service
public class ResendService {
    private static final Logger log = LoggerFactory.getLogger(ResendService.class);

    @Autowired
    private DbService dbService;
    @Autowired
    private MessageSender messageSender;
    @Autowired
    private ForwarderPipeline pipeline;

    @Value("${forwarder.retry.grace.period.ms:10000}")
    private long retryGracePeriodMs;

    @Scheduled(fixedDelayString = "${forwarder.retry.check.interval.ms:10000}")
    public void resendPendingDeliveries() {
        log.debug("Checking for pending deliveries to retry...");

        // Check if we have capacity - if not, skip this retry cycle
        if (!messageSender.hasCapacity()) {
            log.debug("No capacity available ({} active). Skipping retry cycle",
                    messageSender.getActiveRequests());
            return;
        }

        List<DeliveryStatus> pendingDeliveries = dbService.getPendingDeliveries(retryGracePeriodMs);
        if (pendingDeliveries.isEmpty()) {
            return;
        }

        // Batch fetch all data and clients (avoid N+1)
        Set<Long> dataIds = pendingDeliveries.stream()
            .map(DeliveryStatus::getExternalDataId)
            .collect(Collectors.toSet());
        Set<Long> clientIds = pendingDeliveries.stream()
            .map(DeliveryStatus::getClientId)
            .collect(Collectors.toSet());

        Map<Long, ExternalDataTableEntry> dataMap = dbService.getExternalDataByIds(new ArrayList<>(dataIds))
            .stream()
            .collect(Collectors.toMap(ExternalDataTableEntry::getId, d -> d));

        Map<Long, Client> clientMap = dbService.getClientsByIds(new ArrayList<>(clientIds))
            .stream()
            .collect(Collectors.toMap(Client::getId, c -> c));

        // Build ClientBatch objects for the pipeline (reuse same structure)
        Map<Long, ClientBatchBuilder> batchBuilders = new HashMap<>();

        for (DeliveryStatus status : pendingDeliveries) {
            ExternalDataTableEntry data = dataMap.get(status.getExternalDataId());
            Client client = clientMap.get(status.getClientId());

            if (data == null || client == null) {
                log.debug("Skipping pending delivery - data or client deleted: dataId={}, clientId={}",
                    status.getExternalDataId(), status.getClientId());
                continue;
            }

            batchBuilders
                .computeIfAbsent(client.getId(), k -> new ClientBatchBuilder(client))
                .add(data, status);
        }

        // Convert to ClientBatch and dispatch using pipeline
        List<ClientBatch> clientBatches = batchBuilders.values().stream()
            .map(ClientBatchBuilder::build)
            .toList();

        // Dispatch using the pipeline's dispatch stage (reuses all the chunking, capacity checks, result handling)
        pipeline.dispatchStage(clientBatches);
    }

    /**
     * Builder for constructing ClientBatch.
     */
    private static class ClientBatchBuilder {
        private final Client client;
        private final List<ExternalDataTableEntry> dataList = new ArrayList<>();
        private final List<DeliveryStatus> statusList = new ArrayList<>();

        ClientBatchBuilder(Client client) {
            this.client = client;
        }

        void add(ExternalDataTableEntry data, DeliveryStatus status) {
            dataList.add(data);
            statusList.add(status);
        }

        ClientBatch build() {
            return new ClientBatch(client, dataList, statusList);
        }
    }
}

