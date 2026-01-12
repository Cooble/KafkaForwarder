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


@Service
public class DbService {
    private static final Logger log = LoggerFactory.getLogger(DbService.class);

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
     * Saves external data using native JDBC batch upsert - truly idempotent, single DB operation.
     * Uses JDBC batch to execute all upserts in one round trip.
     * Automatically uses H2 MERGE or PostgreSQL INSERT ON CONFLICT based on active profile.
     *
     * IMPORTANT: Only returns NEWLY INSERTED records, not already existing ones.
     * This prevents duplicate DeliveryStatus creation on Kafka message redelivery.
     */
    @Transactional
    public List<ExternalDataTableEntry> saveAllExternalData(List<ExternalDataTableEntry> dataList) {
        if (dataList.isEmpty()) {
            return List.of();
        }

        // First, find which eventIds already exist (to exclude them from the result)
        List<String> eventIds = dataList.stream()
                .map(ExternalDataTableEntry::getEventId)
                .toList();
        Set<String> existingEventIds = new HashSet<>(externalDataRepository.findExistingEventIds(eventIds));

        // Execute batch upsert - all statements in ONE database round trip
        // Use database-specific SQL syntax
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

        jdbcTemplate.batchUpdate(sql, dataList, dataList.size(), (PreparedStatement ps, ExternalDataTableEntry data) -> {
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

        // Only fetch NEWLY INSERTED records (exclude already existing ones)
        List<String> newEventIds = eventIds.stream()
                .filter(id -> !existingEventIds.contains(id))
                .toList();

        if (newEventIds.isEmpty()) {
            log.debug("All {} records already existed - no new data to process", dataList.size());
            return List.of();
        }

        log.debug("Inserted {} new records, {} already existed", newEventIds.size(), existingEventIds.size());
        return externalDataRepository.findByEventIdIn(newEventIds);
    }

    /**
     * Atomically saves external data and creates delivery statuses in a single transaction.
     * This ensures data and statuses are created together, preventing orphaned data entries.
     */
    @Transactional
    public PersistDataResult persistDataAndStatuses(List<ExternalDataTableEntry> dataList, List<ForwarderPipeline.TransformedEvent> transformedEvents, List<String> uniqueTopics) {
        Map<String, List<Client>> topicClientsMap = getClientsByTopics(uniqueTopics);
        List<ExternalDataTableEntry> saved = saveAllExternalData(dataList);
        List<ForwarderPipeline.DeliveryStatusRequest> statusRequests = new ArrayList<>();
        List<Long> orphanDataIds = new ArrayList<>();

        for (int i = 0; i < saved.size(); i++) {
            ExternalDataTableEntry data = saved.get(i);
            ForwarderPipeline.TransformedEvent event = transformedEvents.get(i);
            List<Client> clients = topicClientsMap.getOrDefault(event.topic(), List.of());

            if (clients.isEmpty()) {
                orphanDataIds.add(data.getId());
                continue;
            }

            for (Client client : clients) {
                statusRequests.add(new ForwarderPipeline.DeliveryStatusRequest(
                    data.getId(), client.getId(), event.bornTimeMs(), data, client
                ));
            }
        }

        List<DeliveryStatus> statuses = List.of();
        if (!saved.isEmpty()) {
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
        List<DeliveryStatus> statuses = requests.stream()
                .map(req -> new DeliveryStatus(req.dataId(), req.clientId(), req.bornTimeMs()))
                .toList();

        return deliveryStatusRepository.saveAll(statuses);
    }

    /**
     * Marks delivery statuses as confirmed in batch - FAST version that just marks as confirmed.
     * Cleanup happens separately via scheduled task, not inline.
     */
    @Transactional
    public int markAsConfirmedBatch(List<com.example.forwarder.DeliveryResultHandler.DeliveryUpdate> updates) {
        if (updates.isEmpty()) {
            return 0;
        }

        // Build list of IDs to fetch in ONE query
        Set<String> uniqueKeys = new HashSet<>();
        List<Long> dataIds = new ArrayList<>();
        List<Long> clientIds = new ArrayList<>();

        for (var update : updates) {
            String key = update.dataId() + "_" + update.clientId();
            if (uniqueKeys.add(key)) {
                dataIds.add(update.dataId());
                clientIds.add(update.clientId());
            }
        }

        // Fetch only the statuses we need to update in ONE query
        // Using native query for maximum speed
        int updated = deliveryStatusRepository.markAsConfirmedBulk(dataIds, clientIds);

        return updated;
    }

    public List<DeliveryStatus> getPendingDeliveries(long gracePeriodMs) {
        // Calculate cutoff time - only retry entries older than grace period
        // This prevents resending events that are still awaiting ACK processing
        long cutoffTimeMs = System.currentTimeMillis() - gracePeriodMs;
        return deliveryStatusRepository.findPendingForRetry(cutoffTimeMs);
    }

    public Optional<ExternalDataTableEntry> getExternalDataById(Long id) {
        return externalDataRepository.findById(id);
    }

    public List<ExternalDataTableEntry> getExternalDataByIds(List<Long> ids) {
        return externalDataRepository.findAllById(ids);
    }

    public Optional<Client> getClientById(Long id) {
        return clientRepository.findById(id);
    }

    public List<Client> getClientsByIds(List<Long> ids) {
        return clientRepository.findAllById(ids);
    }

    @Transactional
    public void deleteExternalDataBatch(List<Long> externalDataIds) {
        // Delete all delivery statuses in ONE query instead of multiple
        deliveryStatusRepository.deleteByDataIds(externalDataIds);
        // Delete all external data in one batch
        externalDataRepository.deleteAllById(externalDataIds);
    }


    /**
     * Combined cleanup operation - deletes fully confirmed delivery statuses
     * and then orphaned external data in a single transaction.
     * Returns a record with counts of deleted items.
     */
    @Transactional
    public CleanupResult cleanupConfirmedDataAndOrphans() {
        // Step 1: Delete all delivery statuses for fully confirmed external data
        int deletedStatuses = deliveryStatusRepository.deleteFullyConfirmedDeliveryStatuses();

        // Step 2: Delete external data entries that no longer have any delivery statuses
        // This automatically cleans up the data from step 1
        int deletedData = externalDataRepository.deleteOrphanedData();

        return new CleanupResult(deletedStatuses, deletedData);
    }

    /**
     * Simple record to hold cleanup results.
     */
    public record CleanupResult(int deletedStatuses, int deletedData) {
    }

    /**
     * Result of persisting data and statuses atomically.
     */
    public record PersistDataResult(List<ExternalDataTableEntry> savedData, List<DeliveryStatus> statuses, List<ForwarderPipeline.DeliveryStatusRequest> statusRequests, List<Long> orphanDataIds) {
    }
}
