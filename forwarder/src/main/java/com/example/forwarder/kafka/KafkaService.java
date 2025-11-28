package com.example.forwarder.kafka;

import com.example.forwarder.SendService;
import com.example.forwarder.TransformationService;
import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import com.example.forwarder.model.DeliveryStatus;
import com.example.common.InternalData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaService {
    @Autowired
    private TransformationService transformationService;
    @Autowired
    private SendService sendService;
    @Autowired
    private DbService dbService;

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);

    @KafkaListener(topics = "${kafka.topics}")
    public void listenInternalData(ConsumerRecord<String, InternalData> record) {
        final var message = record.value();
        final var topic = record.topic();
        final var offset = record.offset();  // Kafka offset - guarantees ordering

        log.info("Received event message: {} from topic: {} at offset: {}", message, topic, offset);

        // 1. Transform internal to external data with sequence number
        var externalDataTableEntry = transformationService.transform(message, topic, offset);

        // 2. Save to DB immediately (protection against data loss)
        externalDataTableEntry = dbService.saveExternalData(externalDataTableEntry);
        log.info("Saved external data with ID: {}", externalDataTableEntry.getId());

        // 3. Get all clients subscribed to this topic
        List<Client> subscribedClients = dbService.getClientsByTopic(topic);
        log.info("Found {} clients subscribed to topic {}", subscribedClients.size(), topic);

        if (subscribedClients.isEmpty()) {
            log.warn("No clients subscribed to topic {}, deleting data.", topic);
            dbService.deleteExternalDataAndStatuses(externalDataTableEntry.getId());
            return;
        }

        // 4. Send to clients and create delivery status for each one (wait for confirmation)
        for (final Client client : subscribedClients) {
            sendService.sendToClient(client.getClientIdentifier(), externalDataTableEntry);
            dbService.createDeliveryStatus(externalDataTableEntry.getId(), client.getId());
            
            log.info(
                    "Created delivery status for client {} and data {}",
                    client.getClientIdentifier(),
                    externalDataTableEntry.getId()
            );
        }
    }
}

