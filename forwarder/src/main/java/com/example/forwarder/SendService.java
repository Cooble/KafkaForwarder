package com.example.forwarder;

import com.example.common.ExternalData;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.messaging.simp.SimpMessagingTemplate;

@Service
public class SendService {
    private static final Logger log = LoggerFactory.getLogger(SendService.class);

    private final SimpMessagingTemplate messagingTemplate;

    public SendService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void sendToTopic(String topic, ExternalDataTableEntry data) {
        log.info("Sending data {} to topic {}", data.getId(), topic);

        final var payload = new ExternalData(
            data.getId(),
            data.getMsg(),
            data.getName(),
            data.getExternalNew()
        );

        messagingTemplate.convertAndSend("/topic/" + topic, payload);
    }

    public void sendToClient(String clientId, ExternalDataTableEntry data) {
        log.info("Sending data {} to client {}", data.getId(), clientId);

        final var payload = new ExternalData(
            data.getId(),
            data.getMsg(),
            data.getName(),
            data.getExternalNew()
        );

        messagingTemplate.convertAndSendToUser(clientId, "/queue/data", payload);
    }
}
