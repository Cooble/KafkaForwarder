package com.example.forwarder.db;

import com.example.forwarder.pipeline.ForwarderPipeline;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DbService {
    private static final Logger log = LoggerFactory.getLogger(DbService.class);

    // Safety limit for SQL 'IN' clauses (H2/Postgres limits are usually ~2000-3000)
    private static final int IN_CLAUSE_BATCH_SIZE = 1000;

    private final boolean isPostgres;

    public DbService(@Value("${spring.profiles.active:h2}") String activeProfile) {
        this.isPostgres = "postgres".equalsIgnoreCase(activeProfile);
        log.info("DbService initialized with database profile: {} (isPostgres={})", activeProfile, isPostgres);
    }

    @Autowired
    private ExternalDataRepository externalDataRepository;
    @Autowired
    private ClientRepository clientRepository;
    @Autowired
    private DeliveryStatusRepository deliveryStatusRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Saves external data using native JDBC batch upsert.
     * CHUNKED to prevent parameter limit errors on 'IN' clauses.
     */
    @Transactional
    public List<ExternalDataTableEntry> saveAllExternalData(List<ExternalDataTableEntry> dataList) {
        if (dataList.isEmpty()) {
            return List.of();
        }

        // 1. Identify existing IDs (Chunked)
        List<String> eventIds = dataList.stream()
                .map(ExternalDataTableEntry::getEventId)
                .toList();

        Set<String> existingEventIds = new HashSet<>();
        for (List<String> batch : partition(eventIds, IN_CLAUSE_BATCH_SIZE)) {
            existingEventIds.addAll(externalDataRepository.findExistingEventIds(batch));
        }

        // 2. Perform Batch Upsert (Native JDBC - already safe via loop)
        String sql = isPostgres
                ? """
                INSERT INTO external_data_table_entry (event_id, topic, document_id, customer_id, currency, total_cents, payload_json, sequence_number, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_id) DO UPDATE SET
                    topic = EXCLUDED.topic,
                    document_id = EXCLUDED.document_id,
                    customer_id = EXCLUDED.customer_id,
                    currency = EXCLUDED.currency,
                    total_cents = EXCLUDED.total_cents,
                    payload_json = EXCLUDED.payload_json,
                    sequence_number = EXCLUDED.sequence_number,
                    received_at = EXCLUDED.received_at
                """
                : """
                MERGE INTO external_data_table_entry (event_id, topic, document_id, customer_id, currency, total_cents, payload_json, sequence_number, received_at)
                KEY(event_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        int batchSize = 100;
        for (int i = 0; i < dataList.size(); i += batchSize) {
            int end = Math.min(i + batchSize, dataList.size());
            List<ExternalDataTableEntry> batch = dataList.subList(i, end);
            jdbcTemplate.batchUpdate(sql, batch, batch.size(), (PreparedStatement ps, ExternalDataTableEntry data) -> {
                ps.setString(1, data.getEventId());
                ps.setString(2, data.getTopic());
                ps.setString(3, data.getDocumentId());
                ps.setString(4, data.getCustomerId());
                ps.setString(5, data.getCurrency());
                ps.setLong(6, data.getTotalCents());
                ps.setString(7, data.getPayloadJson());
                ps.setLong(8, data.getSequenceNumber());
                ps.setTimestamp(9, Timestamp.valueOf(data.getReceivedAt()));
            });
        }

        // 3. Fetch newly inserted records (Chunked)
        List<String> newEventIds = eventIds.stream()
                .filter(id -> !existingEventIds.contains(id))
                .toList();

        if (newEventIds.isEmpty()) {
            log.debug("All {} records already existed - no new data to process", dataList.size());
            return List.of();
        }

        log.debug("Inserted {} new records, {} already existed", newEventIds.size(), existingEventIds.size());

        List<ExternalDataTableEntry> result = new ArrayList<>();
        for (List<String> batch : partition(newEventIds, IN_CLAUSE_BATCH_SIZE)) {
            result.addAll(externalDataRepository.findByEventIdIn(batch));
        }
        return result;
    }

    @Transactional
    public PersistDataResult persistDataAndStatuses(List<ExternalDataTableEntry> dataList, List<ForwarderPipeline.TransformedEvent> transformedEvents, List<String> uniqueTopics) {
        Map<String, List<Client>> topicClientsMap = getClientsByTopics(uniqueTopics);
        List<ExternalDataTableEntry> saved = saveAllExternalData(dataList);

        // Map saved entries by EventID for fast, correct lookup
        Map<String, ExternalDataTableEntry> savedMap = saved.stream()
                .collect(Collectors.toMap(ExternalDataTableEntry::getEventId, e -> e));

        List<ForwarderPipeline.DeliveryStatusRequest> statusRequests = new ArrayList<>();
        List<Long> orphanDataIds = new ArrayList<>();

        for (int i = 0; i < dataList.size(); i++) {
            ExternalDataTableEntry inputData = dataList.get(i);

            // If the entry is not in 'savedMap', it was a duplicate/filtered out
            ExternalDataTableEntry savedEntry = savedMap.get(inputData.getEventId());
            if (savedEntry == null) continue;

            ForwarderPipeline.TransformedEvent event = transformedEvents.get(i);
            List<Client> clients = topicClientsMap.getOrDefault(event.topic(), List.of());

            if (clients.isEmpty()) {
                orphanDataIds.add(savedEntry.getId());
                continue;
            }

            for (Client client : clients) {
                statusRequests.add(new ForwarderPipeline.DeliveryStatusRequest(
                        savedEntry.getId(), client.getId(), event.bornTimeMs(), savedEntry, client
                ));
            }
        }

        List<DeliveryStatus> statuses = List.of();
        if (!statusRequests.isEmpty()) {
            statuses = createDeliveryStatusesBatch(statusRequests);
        }
        return new PersistDataResult(saved, statuses, statusRequests, orphanDataIds);
    }

    public Client upsertClient(Client client) {
        Client existingClient = clientRepository.findByClientIdentifier(client.getClientIdentifier());
        if (existingClient != null) {
            existingClient.setClientUrl(client.getClientUrl());
            existingClient.setSubscribedTopics(client.getSubscribedTopics());
            return clientRepository.save(existingClient);
        } else {
            return clientRepository.save(client);
        }
    }

    public List<Client> getAllClients() {
        return clientRepository.findAll();
    }

    @Transactional(readOnly = true)
    public List<Client> getClientsByTopic(String topic) {
        return clientRepository.findAll().stream()
                .filter(client -> client.getSubscribedTopics().contains(topic))
                .toList();
    }

    @Transactional(readOnly = true)
    public Map<String, List<Client>> getClientsByTopics(List<String> topics) {
        // This fetches all clients (usually small #) and filters in memory, so no SQL param limit here.
        List<Client> allClients = clientRepository.findAll();

        Map<String, List<Client>> topicClientsMap = new HashMap<>();
        for (String topic : topics) {
            List<Client> clientsForTopic = allClients.stream()
                    .filter(client -> client.getSubscribedTopics().contains(topic))
                    .toList();
            topicClientsMap.put(topic, clientsForTopic);
        }
        return topicClientsMap;
    }

    public Optional<Client> getClientByIdentifier(String identifier) {
        return Optional.ofNullable(clientRepository.findByClientIdentifier(identifier));
    }

    @Transactional
    public List<DeliveryStatus> createDeliveryStatusesBatch(List<ForwarderPipeline.DeliveryStatusRequest> requests) {
        // Chunking this too, just to be safe if 'saveAll' triggers implicit selects or large batches
        List<DeliveryStatus> allSaved = new ArrayList<>();

        for (List<ForwarderPipeline.DeliveryStatusRequest> batch : partition(requests, IN_CLAUSE_BATCH_SIZE)) {
            List<DeliveryStatus> statuses = batch.stream()
                    .map(req -> new DeliveryStatus(req.dataId(), req.clientId(), req.bornTimeMs()))
                    .toList();
            allSaved.addAll(deliveryStatusRepository.saveAll(statuses));
        }
        return allSaved;
    }

    /**
     * Marks delivery statuses as confirmed in batch.
     * CHUNKED to prevent parameter limit errors on the UPDATE IN (...) clause.
     */
    @Transactional
    public int markAsConfirmedBatch(List<com.example.forwarder.DeliveryResultHandler.DeliveryUpdate> updates) {
        if (updates.isEmpty()) {
            return 0;
        }

        int totalUpdated = 0;

        for (List<com.example.forwarder.DeliveryResultHandler.DeliveryUpdate> batch : partition(updates, IN_CLAUSE_BATCH_SIZE)) {
            Set<String> uniqueKeys = new HashSet<>();
            List<Long> dataIds = new ArrayList<>();
            List<Long> clientIds = new ArrayList<>();

            for (var update : batch) {
                String key = update.dataId() + "_" + update.clientId();
                if (uniqueKeys.add(key)) {
                    dataIds.add(update.dataId());
                    clientIds.add(update.clientId());
                }
            }

            if (!dataIds.isEmpty()) {
                totalUpdated += deliveryStatusRepository.markAsConfirmedBulk(dataIds, clientIds);
            }
        }

        return totalUpdated;
    }

    public List<DeliveryStatus> getPendingDeliveries(long gracePeriodMs) {
        long cutoffTimeMs = System.currentTimeMillis() - gracePeriodMs;
        // This query limits results by nature, so no chunking on input needed here
        return deliveryStatusRepository.findPendingForRetry(cutoffTimeMs);
    }

    public Optional<ExternalDataTableEntry> getExternalDataById(Long id) {
        return externalDataRepository.findById(id);
    }

    /**
     * CHUNKED fix for finding multiple entries by ID.
     */
    public List<ExternalDataTableEntry> getExternalDataByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return List.of();

        List<ExternalDataTableEntry> results = new ArrayList<>();
        for (List<Long> batch : partition(ids, IN_CLAUSE_BATCH_SIZE)) {
            results.addAll(externalDataRepository.findAllById(batch));
        }
        return results;
    }

    public Optional<Client> getClientById(Long id) {
        return clientRepository.findById(id);
    }

    /**
     * CHUNKED fix for finding multiple clients by ID.
     */
    public List<Client> getClientsByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return List.of();

        List<Client> results = new ArrayList<>();
        for (List<Long> batch : partition(ids, IN_CLAUSE_BATCH_SIZE)) {
            results.addAll(clientRepository.findAllById(batch));
        }
        return results;
    }

    @Transactional
    public void deleteExternalDataBatch(List<Long> externalDataIds) {
        // CHUNKED deletion to prevent parameter limit errors
        for (List<Long> batch : partition(externalDataIds, IN_CLAUSE_BATCH_SIZE)) {
            deliveryStatusRepository.deleteByDataIds(batch);
            externalDataRepository.deleteAllById(batch);
        }
    }

    @Transactional
    public CleanupResult cleanupConfirmedDataAndOrphans() {
        int deletedStatuses = deliveryStatusRepository.deleteFullyConfirmedDeliveryStatuses();
        int deletedData = externalDataRepository.deleteOrphanedData();
        return new CleanupResult(deletedStatuses, deletedData);
    }

    /**
     * Helper to split a list into sublists of size L.
     */
    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    public record CleanupResult(int deletedStatuses, int deletedData) {}

    public record PersistDataResult(List<ExternalDataTableEntry> savedData, List<DeliveryStatus> statuses, List<ForwarderPipeline.DeliveryStatusRequest> statusRequests, List<Long> orphanDataIds) {}
}