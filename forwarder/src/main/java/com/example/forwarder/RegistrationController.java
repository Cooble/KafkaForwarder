package com.example.forwarder;

import com.example.common.RegistrationRequest;
import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;

@Controller
public class RegistrationController {
    @Autowired
    private DbService dbService;

    private static final Logger log = LoggerFactory.getLogger(RegistrationController.class);

    @MessageMapping("/register")
    public void registerClient(@Payload RegistrationRequest registrationRequest, SimpMessageHeaderAccessor headerAccessor) {
        // Get client IP from session attributes or headers
        String clientIp = headerAccessor.getFirstNativeHeader("X-Forwarded-For");
        if (clientIp == null) {
            clientIp = "ws-client-" + headerAccessor.getSessionId();
        }
        log.info("Received registration request from client {} at {}: topics={}",
                clientIp, registrationRequest.clientUrl(), registrationRequest.topics());

        dbService.upsertClient(new Client(clientIp, registrationRequest.clientUrl(), registrationRequest.topics()));

        log.info("Client {} registered/updated successfully", clientIp);
    }
}
