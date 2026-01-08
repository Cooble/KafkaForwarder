package com.example.forwarder.kafka;

import com.example.forwarder.DeliveryResultHandler;
import com.example.forwarder.MessageSender;
import com.example.forwarder.MetricsService;
import com.example.forwarder.TransformationService;
import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.model.ExternalDataTableEntry;
import com.example.common.InternalData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaService {
    @Autowired
    private TransformationService transformationService;
    @Autowired
    private MessageSender sendService;
    @Autowired
    private DbService dbService;
    @Autowired
    private DeliveryResultHandler deliveryResultHandler;
    @Autowired
    private MetricsService metricsService;

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);

    @org.springframework.beans.factory.annotation.Value("${forwarder.client.max.batch.size:100}")
    private int maxHttpBatchSize;


    // Thread pool for async DB processing (10 parallel workers)
    private final ExecutorService dbExecutor = Executors.newFixedThreadPool(10, new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "db-worker-" + counter.incrementAndGet());
            t.setDaemon(false);
            return t;
        }
    });

    private final AtomicLong pendingBatches = new AtomicLong(0);

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down DB executor...");
        dbExecutor.shutdown();
        try {
            if (!dbExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                dbExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            dbExecutor.shutdownNow();
        }
    }

    @KafkaListener(topics = "${kafka.topics}", batch = "true")
    public void listenInternalDataBatch(List<ConsumerRecord<String, InternalData>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            return;
        }

        // Track how many events we received from Kafka (not batches, but actual event count)
        metricsService.recordKafkaReceived(records.size());

        // Process asynchronously - don't block Kafka consumer thread
        pendingBatches.incrementAndGet();
        CompletableFuture.runAsync(() -> processBatchAsync(records, acknowledgment), dbExecutor)
            .exceptionally(ex -> {
                log.error("Error processing batch of {} records - will NOT commit offset", records.size(), ex);
                pendingBatches.decrementAndGet();
                // Offset NOT committed → Kafka will redeliver these messages on restart
                return null;
            });
    }

    private void processBatchAsync(List<ConsumerRecord<String, InternalData>> records, Acknowledgment acknowledgment) {
        try {
            // 1. Transform all events (fast, in-memory)
            List<ExternalDataTableEntry> externalDataList = new ArrayList<>();
            List<EventRecord> eventRecords = new ArrayList<>();

            for (ConsumerRecord<String, InternalData> record : records) {
                InternalData message = record.value();
                long bornTimeMs = message.bornTimeMs() > 0 ? message.bornTimeMs() : System.currentTimeMillis();

                ExternalDataTableEntry entry = transformationService.transform(
                    message, record.topic(), record.offset()
                );
                externalDataList.add(entry);
                eventRecords.add(new EventRecord(message, record.topic(), record.offset(), bornTimeMs));
            }

            // 2. SINGLE DB CALL: Batch save all external data
            List<ExternalDataTableEntry> savedData = dbService.saveAllExternalData(externalDataList);

            // 3. SINGLE DB CALL: Get all clients for all unique topics
            List<String> uniqueTopics = eventRecords.stream()
                .map(EventRecord::topic)
                .distinct()
                .toList();
            Map<String, List<Client>> topicClientsMap = dbService.getClientsByTopics(uniqueTopics);

            // 4. Prepare all delivery status requests in memory
            List<DeliveryStatusRequest> allStatusRequests = new ArrayList<>();
            List<Long> dataIdsToDelete = new ArrayList<>();

            for (int i = 0; i < savedData.size(); i++) {
                ExternalDataTableEntry data = savedData.get(i);
                EventRecord event = eventRecords.get(i);

                List<Client> clients = topicClientsMap.getOrDefault(event.topic, List.of());

                if (clients.isEmpty()) {
                    dataIdsToDelete.add(data.getId());
                    continue;
                }

                for (Client client : clients) {
                    allStatusRequests.add(new DeliveryStatusRequest(
                        data.getId(), client.getId(), event.bornTimeMs, data, client
                    ));
                }
            }

            // Delete data with no subscribers (if any) - async, don't block
            if (!dataIdsToDelete.isEmpty()) {
                CompletableFuture.runAsync(() -> {
                    dbService.deleteExternalDataBatch(dataIdsToDelete);
                }, dbExecutor);
            }

            // 5. SINGLE DB CALL: Batch create ALL delivery statuses
            List<DeliveryStatus> allStatuses = dbService.createDeliveryStatusesBatch(allStatusRequests);

            // 6. Group events by client for batch sending (instead of sending one-by-one!)
            Map<Long, List<BatchDeliveryItem>> eventsByClient = new java.util.HashMap<>();
            for (int i = 0; i < allStatusRequests.size(); i++) {
                var req = allStatusRequests.get(i);
                var status = allStatuses.get(i);

                eventsByClient.computeIfAbsent(req.client.getId(), k -> new ArrayList<>())
                    .add(new BatchDeliveryItem(req.data, status, req.client));
            }

            // 7. Send batched HTTP requests per client (split into chunks to avoid huge payloads)
            for (var entry : eventsByClient.entrySet()) {
                List<BatchDeliveryItem> items = entry.getValue();
                if (items.isEmpty()) continue;

                // All items have the same client
                Client client = items.get(0).client;

                // Split into chunks of maxHttpBatchSize to prevent payloads that are too large
                for (int i = 0; i < items.size(); i += maxHttpBatchSize) {
                    // Check HTTP capacity before each batch - if exhausted, stop and let retry handle it
                    if (!sendService.hasCapacity()) {
                        log.debug("HTTP capacity exhausted ({} active). Stopping batch send - remaining will be retried",
                                sendService.getActiveRequests());
                        break;  // Stop sending this client's batches - retry will pick them up
                    }

                    int end = Math.min(i + maxHttpBatchSize, items.size());
                    List<BatchDeliveryItem> chunk = items.subList(i, end);

                    List<ExternalDataTableEntry> dataList = chunk.stream()
                        .map(item -> item.data)
                        .toList();
                    List<DeliveryStatus> statusList = chunk.stream()
                        .map(item -> item.status)
                        .toList();

                    sendBatchToClientAsync(dataList, client, statusList);
                }
            }


            // ✅ CRITICAL: Only NOW commit the Kafka offset (data is safely in DB)
            // If we crash before this line, Kafka will redeliver the messages
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process batch, offset will NOT be committed - Kafka will redeliver", e);
            // Offset not committed → Kafka will redeliver these messages on restart
            throw e; // Re-throw to trigger the exceptionally handler
        } finally {
            pendingBatches.decrementAndGet();
        }
    }

    private void sendBatchToClientAsync(List<ExternalDataTableEntry> dataList, Client client, List<DeliveryStatus> statusList) {
        // Track send attempt for metrics
        metricsService.recordSendAttempt(dataList.size());

        sendService.sendBatchToClient(dataList, client).thenAccept(result -> {
            // Handle result for all events in the batch
            for (int i = 0; i < statusList.size(); i++) {
                DeliveryStatus status = statusList.get(i);
                Long dataId = dataList.get(i).getId();
                // Create individual SendResult for each event in the batch
                MessageSender.SendResult individualResult = new MessageSender.SendResult(result.success(), dataId, result.clientId());
                deliveryResultHandler.handleSendResult(individualResult, status, client.getClientIdentifier());
            }

            // Track failures if the batch send failed
            if (!result.success()) {
                metricsService.recordSendFailure(dataList.size());
            }
        }).exceptionally(ex -> {
            // Track failures for exception cases
            metricsService.recordSendFailure(dataList.size());

            // Handle exception for all events in the batch
            for (int i = 0; i < statusList.size(); i++) {
                deliveryResultHandler.handleSendException(dataList.get(i).getId(), client.getClientIdentifier(), statusList.get(i), ex);
            }
            return null;
        });
    }


    // Inner classes to hold event data
    private record EventRecord(InternalData message, String topic, Long offset, long bornTimeMs) {}
    private record BatchDeliveryItem(ExternalDataTableEntry data, DeliveryStatus status, Client client) {}

    public record DeliveryStatusRequest(Long dataId, Long clientId, long bornTimeMs,
                                        ExternalDataTableEntry data, Client client) {}
}

