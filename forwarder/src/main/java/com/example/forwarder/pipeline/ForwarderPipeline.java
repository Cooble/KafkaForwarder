package com.example.forwarder.pipeline;

import com.example.forwarder.DeliveryResultHandler;
import com.example.forwarder.MessageSender;
import com.example.forwarder.MetricsService;
import com.example.forwarder.TransformationService;
import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.model.ExternalDataTableEntry;
import com.example.forwarder.registry.ClientRegistry;
import com.example.common.InternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Central pipeline for processing events through transform → persist → dispatch stages.
 * Replaces the scattered logic previously in KafkaService.processBatchAsync().
 *
 * Used by:
 * - KafkaService: for new events from Kafka
 * - ResendService: for retrying pending deliveries (dispatch stage only)
 */
@Component
public class ForwarderPipeline {
    private static final Logger log = LoggerFactory.getLogger(ForwarderPipeline.class);

    @Autowired
    private TransformationService transformationService;
    @Autowired
    private DbService dbService;
    @Autowired
    private ClientRegistry clientRegistry;
    @Autowired
    private MessageSender messageSender;
    @Autowired
    private DeliveryResultHandler deliveryResultHandler;
    @Autowired
    private MetricsService metricsService;
    @Autowired
    private ForwarderExecutorService executorService;

    @Value("${forwarder.client.max.batch.size:100}")
    private int maxBatchSize;

    // ========== STAGE 1: TRANSFORM ==========

    /**
     * Transform stage: Convert Kafka records to external data entries.
     * Pure in-memory transformation, no I/O.
     *
     * @param records Raw Kafka records with InternalData
     * @return List of transformed entries ready for persistence
     */
    public TransformResult transformStage(List<KafkaEventInput> records) {
        List<ExternalDataTableEntry> externalDataList = new ArrayList<>(records.size());
        List<TransformedEvent> transformedEvents = new ArrayList<>(records.size());

        for (KafkaEventInput record : records) {
            InternalData message = record.message();
            long bornTimeMs = message.bornTimeMs() > 0 ? message.bornTimeMs() : System.currentTimeMillis();

            ExternalDataTableEntry entry = transformationService.transform(
                message, record.topic(), record.offset()
            );
            externalDataList.add(entry);
            transformedEvents.add(new TransformedEvent(record.topic(), record.offset(), bornTimeMs));
        }

        return new TransformResult(externalDataList, transformedEvents);
    }

    // ========== STAGE 2: PERSIST ==========

    /**
     * Persist stage: Save external data and create delivery statuses.
     * Two DB calls: batch save data, batch create statuses.
     *
     * @param transformResult Output from transform stage
     * @return Persisted data with delivery statuses, grouped by client
     */
    public PersistResult persistStage(TransformResult transformResult) {
        List<ExternalDataTableEntry> externalDataList = transformResult.externalDataList();
        List<TransformedEvent> transformedEvents = transformResult.transformedEvents();

        // 1. Batch save all external data (single DB call)
        List<ExternalDataTableEntry> savedData = dbService.saveAllExternalData(externalDataList);

        if (savedData.isEmpty()) {
            return PersistResult.empty();
        }

        // 2. Get clients for all unique topics (from in-memory cache)
        List<String> uniqueTopics = transformedEvents.stream()
            .map(TransformedEvent::topic)
            .distinct()
            .toList();
        Map<String, List<Client>> topicClientsMap = clientRegistry.getClientsByTopics(uniqueTopics);

        // 3. Build delivery status requests and identify orphan data
        List<DeliveryStatusRequest> statusRequests = new ArrayList<>();
        List<Long> orphanDataIds = new ArrayList<>();

        for (int i = 0; i < savedData.size(); i++) {
            ExternalDataTableEntry data = savedData.get(i);
            TransformedEvent event = transformedEvents.get(i);
            List<Client> clients = topicClientsMap.getOrDefault(event.topic(), List.of());

            if (clients.isEmpty()) {
                orphanDataIds.add(data.getId());
                continue;
            }

            for (Client client : clients) {
                statusRequests.add(new DeliveryStatusRequest(
                    data.getId(), client.getId(), event.bornTimeMs(), data, client
                ));
            }
        }

        // 4. Delete orphan data async (no subscribers)
        if (!orphanDataIds.isEmpty()) {
            executorService.submitCleanupTask(() -> dbService.deleteExternalDataBatch(orphanDataIds));
        }

        // 5. Batch create all delivery statuses (single DB call)
        List<DeliveryStatus> statuses = dbService.createDeliveryStatusesBatch(statusRequests);

        // 6. Group by client for dispatch stage
        List<ClientBatch> clientBatches = groupByClient(statusRequests, statuses);

        return new PersistResult(savedData, statuses, clientBatches);
    }

    // ========== STAGE 3: DISPATCH ==========

    /**
     * Dispatch stage: Send batches to clients.
     * Respects capacity limits and splits large batches.
     *
     * Used by both KafkaService (new events) and ResendService (retries).
     *
     * @param clientBatches Batches grouped by client from persist stage
     */
    public void dispatchStage(List<ClientBatch> clientBatches) {
        for (ClientBatch batch : clientBatches) {
            dispatchToClient(batch);
        }
    }

    /**
     * Dispatch a single client's batch, splitting into chunks if needed.
     * Public so ResendService can call it directly for retry batches.
     */
    public void dispatchToClient(ClientBatch batch) {
        Client client = batch.client();
        List<ExternalDataTableEntry> dataList = batch.dataList();
        List<DeliveryStatus> statusList = batch.statusList();

        if (dataList.isEmpty()) {
            return;
        }

        // Split into chunks to prevent oversized payloads
        for (int i = 0; i < dataList.size(); i += maxBatchSize) {
            // Check capacity before each chunk
            if (!messageSender.hasCapacity()) {
                log.debug("Capacity exhausted ({} active). Stopping dispatch - remaining will be retried",
                    messageSender.getActiveRequests());
                break;
            }

            int end = Math.min(i + maxBatchSize, dataList.size());
            List<ExternalDataTableEntry> chunkData = dataList.subList(i, end);
            List<DeliveryStatus> chunkStatus = statusList.subList(i, end);

            sendChunkAsync(chunkData, client, chunkStatus);
        }
    }

    // ========== INTERNAL HELPERS ==========

    private void sendChunkAsync(List<ExternalDataTableEntry> dataList, Client client, List<DeliveryStatus> statusList) {
        metricsService.recordSendAttempt(dataList.size());

        messageSender.sendBatchToClient(dataList, client).thenAccept(result -> {
            // Handle result for all events in the batch
            for (int i = 0; i < statusList.size(); i++) {
                DeliveryStatus status = statusList.get(i);
                Long dataId = dataList.get(i).getId();
                MessageSender.SendResult individualResult = new MessageSender.SendResult(
                    result.success(), dataId, result.clientId()
                );
                deliveryResultHandler.handleSendResult(individualResult, status, client.getClientIdentifier());
            }

            if (!result.success()) {
                metricsService.recordSendFailure(dataList.size());
            }
        }).exceptionally(ex -> {
            metricsService.recordSendFailure(dataList.size());
            for (int i = 0; i < statusList.size(); i++) {
                deliveryResultHandler.handleSendException(
                    dataList.get(i).getId(), client.getClientIdentifier(), statusList.get(i), ex
                );
            }
            return null;
        });
    }

    private List<ClientBatch> groupByClient(List<DeliveryStatusRequest> requests, List<DeliveryStatus> statuses) {
        Map<Long, ClientBatchBuilder> buildersByClient = new HashMap<>();

        for (int i = 0; i < requests.size(); i++) {
            DeliveryStatusRequest req = requests.get(i);
            DeliveryStatus status = statuses.get(i);

            buildersByClient
                .computeIfAbsent(req.client().getId(), k -> new ClientBatchBuilder(req.client()))
                .add(req.data(), status);
        }

        return buildersByClient.values().stream()
            .map(ClientBatchBuilder::build)
            .toList();
    }

    // ========== DATA CLASSES ==========

    /**
     * Input from Kafka listener.
     */
    public record KafkaEventInput(InternalData message, String topic, Long offset) {}

    /**
     * Output from transform stage.
     */
    public record TransformResult(
        List<ExternalDataTableEntry> externalDataList,
        List<TransformedEvent> transformedEvents
    ) {}

    /**
     * Represents a transformed event with metadata.
     */
    public record TransformedEvent(String topic, Long offset, long bornTimeMs) {}

    /**
     * Output from persist stage.
     */
    public record PersistResult(
        List<ExternalDataTableEntry> savedData,
        List<DeliveryStatus> statuses,
        List<ClientBatch> clientBatches
    ) {
        public static PersistResult empty() {
            return new PersistResult(List.of(), List.of(), List.of());
        }

        public boolean isEmpty() {
            return savedData.isEmpty();
        }
    }

    /**
     * Request to create a delivery status.
     */
    public record DeliveryStatusRequest(
        Long dataId,
        Long clientId,
        long bornTimeMs,
        ExternalDataTableEntry data,
        Client client
    ) {}

    /**
     * A batch of events for a single client.
     */
    public record ClientBatch(
        Client client,
        List<ExternalDataTableEntry> dataList,
        List<DeliveryStatus> statusList
    ) {}

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

