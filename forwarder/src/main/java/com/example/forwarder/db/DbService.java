package com.example.forwarder.db;

import com.example.forwarder.kafka.KafkaService;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;


@Service
public class DbService {
    private static final Logger log = LoggerFactory.getLogger(DbService.class);

    @Autowired
    private ExternalDataRepository externalDataRepository;
    @Autowired
    private ClientRepository clientRepository;
    @Autowired
    private DeliveryStatusRepository deliveryStatusRepository;

    @Transactional
    public List<ExternalDataTableEntry> saveAllExternalData(List<ExternalDataTableEntry> dataList) {
        return externalDataRepository.saveAll(dataList);
    }

    public void upsertClient(Client client) {
        Client existingClient = clientRepository.findByClientIdentifier(client.getClientIdentifier());
        if (existingClient != null) {
            existingClient.setClientUrl(client.getClientUrl());
            existingClient.setSubscribedTopics(client.getSubscribedTopics());
            clientRepository.save(existingClient);
        } else {
            clientRepository.save(client);
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
    public List<DeliveryStatus> createDeliveryStatusesBatch(List<KafkaService.DeliveryStatusRequest> requests) {
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

    public List<DeliveryStatus> getPendingDeliveries() {
        // Native SQL query already orders by externalDataId and id - no need for Java sorting!
        return deliveryStatusRepository.findPendingOrderedByDataId();
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
}

