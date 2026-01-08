package com.example.forwarder.websocket;

import com.example.common.AckMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${forwarder.websocket.ack.timeout.ms:10000}")
    private long ackTimeoutMs;

    private final Map<Long, WebSocketSessionSender> senders = new ConcurrentHashMap<>();

    public void addSession(Long clientId, String clientIdentifier, WebSocketSession session) {
        // Create dedicated sender thread for this session
        WebSocketSessionSender sender = new WebSocketSessionSender(
            clientId, clientIdentifier, session, objectMapper, ackTimeoutMs
        );

        // Remove old sender if exists (reconnect case)
        WebSocketSessionSender oldSender = senders.put(clientId, sender);
        if (oldSender != null) {
            log.warn("Replacing existing sender for client {} - shutting down old sender", clientId);
            oldSender.shutdown();
        }

        log.info("WebSocket session added for client {} ({}) with dedicated sender thread",
                clientId, clientIdentifier);
    }

    public void removeSession(Long clientId) {
        WebSocketSessionSender removed = senders.remove(clientId);
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
        WebSocketSessionSender sender = senders.get(clientId);
        if (sender != null) {
            sender.handleAck(ackMessage);
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

