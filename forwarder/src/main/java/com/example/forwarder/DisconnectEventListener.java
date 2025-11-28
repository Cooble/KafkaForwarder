package com.example.forwarder;

import com.example.forwarder.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

@Component
public class DisconnectEventListener {
    private static final Logger log = LoggerFactory.getLogger(DisconnectEventListener.class);

    @Autowired
    private DbService dbService;

    @EventListener
    public void handleSessionDisconnect(SessionDisconnectEvent event) {
        String sessionId = event.getSessionId();
        log.info("WebSocket session disconnected: {}", sessionId);
        dbService.handleClientDisconnect(sessionId);
    }
}