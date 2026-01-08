package com.example.forwarder.websocket;

import com.example.common.AckMessage;
import com.example.forwarder.DeliveryResultHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages active WebSocket sessions and their dedicated sender threads.
 * Each session gets its own sender thread with a dedicated queue.
 */
@Component
public class WebSocketSessionManager {
    private static final Logger log = LoggerFactory.getLogger(WebSocketSessionManager.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DeliveryResultHandler deliveryResultHandler;

    private final Map<Long, WebSocketSessionSender> senders = new ConcurrentHashMap<>();
    private final Map<Long, String> clientIdentifiers = new ConcurrentHashMap<>();

    public void addSession(Long clientId, String clientIdentifier, WebSocketSession session) {
        // Create dedicated sender thread for this session
        WebSocketSessionSender sender = new WebSocketSessionSender(
            clientId, clientIdentifier, session, objectMapper
        );

        // Remove old sender if exists (reconnect case)
        WebSocketSessionSender oldSender = senders.put(clientId, sender);
        if (oldSender != null) {
            log.warn("Replacing existing sender for client {} - shutting down old sender", clientId);
            oldSender.shutdown();
        }

        // Store client identifier for ACK handling
        clientIdentifiers.put(clientId, clientIdentifier);

        log.info("WebSocket session added for client {} ({}) with dedicated sender thread",
                clientId, clientIdentifier);
    }

    public void removeSession(Long clientId) {
        WebSocketSessionSender removed = senders.remove(clientId);
        clientIdentifiers.remove(clientId);
        if (removed != null) {
            removed.shutdown();
            log.info("WebSocket session removed for client {}", clientId);
        }
    }

    public WebSocketSessionSender getSender(Long clientId) {
        return senders.get(clientId);
    }

    public boolean hasSession(Long clientId) {
        return senders.containsKey(clientId);
    }

    public int getActiveSessionCount() {
        return senders.size();
    }

    public void handleAck(Long clientId, AckMessage ackMessage) {
        // Update DB directly - mark deliveries as confirmed
        // This is the key change: ACKs update the DB, ResendService handles retries
        String clientIdentifier = clientIdentifiers.get(clientId);
        if (clientIdentifier == null) {
            log.warn("Received ACK for unknown client {}", clientId);
            return;
        }

        // Queue each data ID for DB update via DeliveryResultHandler
        for (Long dataId : ackMessage.dataIds()) {
            deliveryResultHandler.handleWebSocketAck(dataId, clientId);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down all WebSocket senders - {} active sessions", senders.size());
        senders.values().forEach(WebSocketSessionSender::shutdown);
        senders.clear();
        log.info("All WebSocket senders shut down");
    }
}

