package com.example.forwarder;

import com.example.common.ConfirmationRequest;
import com.example.forwarder.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;

@Controller
public class ConfirmationController {
    @Autowired
    private DbService dbService;

    private static final Logger log = LoggerFactory.getLogger(ConfirmationController.class);

    @MessageMapping("/confirm")
    public void confirmDelivery(@Payload final ConfirmationRequest request) {
        log.info("Received WS confirmation for data ID {} from client {}", request.dataId(), request.clientIdentifier());

        if (dbService.markAsConfirmed(request.dataId(), request.clientIdentifier())) {
            log.info("Successfully confirmed delivery of data {} to client {}", request.dataId(), request.clientIdentifier());
        } else {
            log.warn("Failed to confirm delivery - data or client not found: data={}, client={}", request.dataId(), request.clientIdentifier());
        }
    }
}

