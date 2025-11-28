package com.example.forwarder.db;

import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

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

    public void upsertClient(final Client client) {
        // Check if client with this identifier already exists (e.g., from restart/reconnect)
        final Client existingClient = clientRepository.findByClientIdentifier(client.getClientIdentifier());
        
        if (existingClient != null) {
            // Update existing client instead of creating duplicate
            log.info("Client {} already registered, updating registration", client.getClientIdentifier());
            existingClient.setSubscribedTopics(client.getSubscribedTopics());
            clientRepository.save(existingClient);
        } else {
            // New client, save as is
            log.info("Registering new client {}", client.getClientIdentifier());
            clientRepository.save(client);
        }
    }

    public List<Client> getAllClients() {
        return clientRepository.findAll();
    }

    @Transactional(readOnly = true)
    public List<Client> getClientsByTopic(final String topic) {
        return clientRepository.findAll().stream()
                .filter(client -> client.getSubscribedTopics().contains(topic))
                .toList();
    }

    public Optional<Client> getClientByIdentifier(final String identifier) {
        return Optional.ofNullable(clientRepository.findByClientIdentifier(identifier));
    }

    public DeliveryStatus createDeliveryStatus(final Long externalDataId, final Long clientId) {
        final DeliveryStatus status = new DeliveryStatus(externalDataId, clientId);
        return deliveryStatusRepository.save(status);
    }

    @Transactional
    public boolean markAsConfirmed(final Long dataId, final String clientIdentifier) {
        final Client client = clientRepository.findByClientIdentifier(clientIdentifier);
        
        if (client == null) {
            log.debug("client {} not found", clientIdentifier);
            return false;
        }
        
        return markAsConfirmedByClientId(dataId, client.getId());
    }

    @Transactional
    private boolean markAsConfirmedByClientId(final Long dataId, final Long clientId) {
        final Optional<DeliveryStatus> statusOpt = deliveryStatusRepository.findByExternalDataIdAndClientId(dataId, clientId);

        if (statusOpt.isPresent()) {
            DeliveryStatus status = statusOpt.get();
            
            status.setConfirmed(true);
            deliveryStatusRepository.save(status);

            // Check if all clients have confirmed
            long totalClients = deliveryStatusRepository.countByExternalDataId(dataId);
            long confirmedClients = deliveryStatusRepository.countByExternalDataIdAndConfirmedTrue(dataId);

            if (totalClients == confirmedClients) {
                // All clients confirmed, clean up
                deleteExternalDataAndStatuses(dataId);
            }
            return true;
        }
        return false;
    }

    public List<DeliveryStatus> getPendingDeliveries(final int secondsAgo, final int maxAttempts) {
        final LocalDateTime threshold = LocalDateTime.now().minusSeconds(secondsAgo);
        return deliveryStatusRepository.findByConfirmedFalseAndLastAttemptBefore(threshold)
                .stream()
                .filter(status -> status.getAttemptCount() < maxAttempts)
                .sorted((a, b) -> {
                    // Sort by ExternalData sequence number to maintain order
                    Optional<ExternalDataTableEntry> dataA = getExternalDataById(a.getExternalDataId());
                    Optional<ExternalDataTableEntry> dataB = getExternalDataById(b.getExternalDataId());
                    if (dataA.isPresent() && dataB.isPresent()) {
                        Long seqA = dataA.get().getSequenceNumber();
                        Long seqB = dataB.get().getSequenceNumber();
                        if (seqA != null && seqB != null) {
                            return seqA.compareTo(seqB);  // Ascending order (oldest first)
                        }
                    }
                    return 0;
                })
                .toList();
    }

    public DeliveryStatus updateDeliveryStatus(final DeliveryStatus status) {
        return deliveryStatusRepository.save(status);
    }

    public Optional<ExternalDataTableEntry> getExternalDataById(final Long id) {
        return externalDataRepository.findById(id);
    }

    public Optional<Client> getClientById(final Long id) {
        return clientRepository.findById(id);
    }

    @Transactional
    public void deleteExternalDataAndStatuses(final Long externalDataId) {
        deliveryStatusRepository.deleteByExternalDataId(externalDataId);
        externalDataRepository.deleteById(externalDataId);
    }

    @Transactional
    public void deleteFailedDeliveries(final int maxAttempts) {
        // TODO maybe delete only statuses that exceeded and then check for the data without any status (and then delete it to)
        deliveryStatusRepository
                .findAll()
                .stream()
                .filter(status -> !status.isConfirmed() && status.getAttemptCount() >= maxAttempts)
                .map(DeliveryStatus::getExternalDataId)
                .distinct()
                .forEach(this::deleteExternalDataAndStatuses);
    }
}
