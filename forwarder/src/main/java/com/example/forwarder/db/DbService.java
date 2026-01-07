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

    public ExternalDataTableEntry saveExternalData(ExternalDataTableEntry data) {
        return externalDataRepository.save(data);
    }

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

    public DeliveryStatus createDeliveryStatus(Long externalDataId, Long clientId) {
        DeliveryStatus status = new DeliveryStatus(externalDataId, clientId);
        return deliveryStatusRepository.save(status);
    }

    public List<DeliveryStatus> createDeliveryStatuses(Long externalDataId, List<Long> clientIds, long bornTimeMs) {
        return deliveryStatusRepository.saveAll(
                clientIds.stream()
                        .map(clientId -> new DeliveryStatus(externalDataId, clientId, bornTimeMs))
                        .toList()
        );
    }

    @Transactional
    public List<DeliveryStatus> createDeliveryStatusesBatch(List<KafkaService.DeliveryStatusRequest> requests) {
        List<DeliveryStatus> statuses = requests.stream()
                .map(req -> new DeliveryStatus(req.dataId(), req.clientId(), req.bornTimeMs()))
                .toList();

        return deliveryStatusRepository.saveAll(statuses);
    }

    @Transactional
    public boolean markAsConfirmed(Long dataId, String clientIdentifier) {
        Client client = clientRepository.findByClientIdentifier(clientIdentifier);
        if (client == null) {
            return false;
        }
        return markAsConfirmedByClientId(dataId, client.getId());
    }

    @Transactional
    public boolean markAsConfirmedByClientId(Long dataId, Long clientId) {
        Optional<DeliveryStatus> statusOpt = deliveryStatusRepository
                .findByExternalDataIdAndClientId(dataId, clientId);

        if (statusOpt.isEmpty()) {
            // Delivery status doesn't exist - likely already deleted by another thread
            log.debug("Delivery status for data {} and client {} not found - already processed", dataId, clientId);
            return false;
        }

        DeliveryStatus status = statusOpt.get();

        // Don't mark as confirmed if already confirmed (idempotent)
        if (status.isConfirmed()) {
            log.debug("Delivery status {} already confirmed - skipping", status.getId());
            return true;
        }

        status.setConfirmed(true);
        deliveryStatusRepository.save(status);

        // Check if all deliveries are confirmed by fetching actual list (more reliable than counts)
        List<DeliveryStatus> allStatuses = deliveryStatusRepository.findByExternalDataId(dataId);
        boolean allConfirmed = allStatuses.stream().allMatch(DeliveryStatus::isConfirmed);

        if (allConfirmed && !allStatuses.isEmpty()) {
            // All deliveries confirmed - safe to delete data and delivery statuses
            try {
                deliveryStatusRepository.deleteByExternalDataId(dataId);
                externalDataRepository.deleteById(dataId);
                log.debug("Successfully deleted data {} and all delivery statuses after all confirmations", dataId);
            } catch (Exception e) {
                // Ignore deletion errors - likely already deleted by concurrent thread
                log.debug("Failed to delete data {} - may have been already deleted concurrently: {}", dataId, e.getMessage());
            }
        }
        return true;
    }

    public List<DeliveryStatus> getPendingDeliveries() {
        return deliveryStatusRepository.findByConfirmedFalse()
                .stream()
                .sorted((a, b) -> {
                    Optional<ExternalDataTableEntry> dataA = getExternalDataById(a.getExternalDataId());
                    Optional<ExternalDataTableEntry> dataB = getExternalDataById(b.getExternalDataId());

                    // Handle missing data - missing data sorts last
                    if (dataA.isEmpty() && dataB.isEmpty()) return Long.compare(a.getId(), b.getId());
                    if (dataA.isEmpty()) return 1;  // A missing, sorts after B
                    if (dataB.isEmpty()) return -1; // B missing, sorts after A

                    Long seqA = dataA.get().getSequenceNumber();
                    Long seqB = dataB.get().getSequenceNumber();

                    // Handle null sequence numbers - null sorts last
                    if (seqA == null && seqB == null) return Long.compare(a.getId(), b.getId());
                    if (seqA == null) return 1;  // A null, sorts after B
                    if (seqB == null) return -1; // B null, sorts after A

                    // Both present and non-null - compare by sequence number
                    int seqCompare = seqA.compareTo(seqB);
                    if (seqCompare != 0) return seqCompare;

                    // Same sequence - use ID as tiebreaker for stable sort
                    return Long.compare(a.getId(), b.getId());
                })
                .toList();
    }

    public DeliveryStatus updateDeliveryStatus(DeliveryStatus status) {
        return deliveryStatusRepository.save(status);
    }

    public Optional<ExternalDataTableEntry> getExternalDataById(Long id) {
        return externalDataRepository.findById(id);
    }

    public Optional<Client> getClientById(Long id) {
        return clientRepository.findById(id);
    }

    @Transactional
    public void deleteExternalDataAndStatuses(Long externalDataId) {
        deliveryStatusRepository.deleteByExternalDataId(externalDataId);
        externalDataRepository.deleteById(externalDataId);
    }

    @Transactional
    public void deleteExternalDataBatch(List<Long> externalDataIds) {
        // Delete all delivery statuses in ONE query instead of multiple
        deliveryStatusRepository.deleteByExternalDataIdIn(externalDataIds);
        // Delete all external data in one batch
        externalDataRepository.deleteAllById(externalDataIds);
    }

    @Transactional
    public void deleteDeliveryStatus(Long deliveryStatusId) {
        deliveryStatusRepository.deleteById(deliveryStatusId);
    }
}

