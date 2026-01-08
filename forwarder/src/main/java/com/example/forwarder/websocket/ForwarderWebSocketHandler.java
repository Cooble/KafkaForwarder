package com.example.forwarder.websocket;

import com.example.common.AckMessage;
import com.example.common.RegistrationRequest;
import com.example.common.WebSocketMessage;
import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnProperty(name = "forwarder.transport.mode", havingValue = "websocket")
public class ForwarderWebSocketHandler extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(ForwarderWebSocketHandler.class);

    @Autowired
    private WebSocketSessionManager sessionManager;

    @Autowired
    private WebSocketSendService sendService;

    @Autowired
    private DbService dbService;

    @Autowired
    private ObjectMapper objectMapper;

    private final Map<String, Long> sessionToClientId = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("WebSocket connection established: {}", session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            WebSocketMessage wsMessage = objectMapper.readValue(message.getPayload(), WebSocketMessage.class);

            switch (wsMessage.type()) {
                case REGISTER:
                    handleRegistration(session, wsMessage);
                    break;
                case ACK:
                    handleAcknowledgment(session, wsMessage);
                    break;
                case PING:
                    handlePing(session);
                    break;
                default:
                    log.warn("Unknown message type from session {}: {}", session.getId(), wsMessage.type());
            }
        } catch (Exception e) {
            log.error("Error handling WebSocket message from session {}: {}", session.getId(), e.getMessage(), e);
        }
    }

    private void handleRegistration(WebSocketSession session, WebSocketMessage wsMessage) throws Exception {
        RegistrationRequest registration = objectMapper.convertValue(wsMessage.payload(), RegistrationRequest.class);

        String clientIdentifier = session.getRemoteAddress() != null
            ? session.getRemoteAddress().getAddress().getHostAddress()
            : "unknown";

        log.info("Client registering via WebSocket: {} for topics {}", clientIdentifier, registration.topics());

        Client client = new Client(clientIdentifier, "", registration.topics());
        Client savedClient = dbService.upsertClient(client);

        // Pass client identifier to session manager for logging
        sessionManager.addSession(savedClient.getId(), savedClient.getClientIdentifier(), session);
        sessionToClientId.put(session.getId(), savedClient.getId());

        log.info("Client {} registered with ID {}", clientIdentifier, savedClient.getId());
    }

    private void handleAcknowledgment(WebSocketSession session, WebSocketMessage wsMessage) throws Exception {
        Long clientId = sessionToClientId.get(session.getId());
        if (clientId == null) {
            log.warn("Received ACK from unregistered session: {}", session.getId());
            return;
        }

        AckMessage ackMessage = objectMapper.convertValue(wsMessage.payload(), AckMessage.class);
        // Use session manager to handle ACK - it routes to the correct sender
        sessionManager.handleAck(clientId, ackMessage);

        log.debug("Received ACK from client {} for {} events", clientId, ackMessage.dataIds().size());
    }

    private void handlePing(WebSocketSession session) throws Exception {
        WebSocketMessage pongMessage = new WebSocketMessage(WebSocketMessage.MessageType.PONG, null);
        String jsonMessage = objectMapper.writeValueAsString(pongMessage);
        session.sendMessage(new TextMessage(jsonMessage));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        log.info("WebSocket connection closed: {} with status {}", session.getId(), status);

        Long clientId = sessionToClientId.remove(session.getId());
        if (clientId != null) {
            sessionManager.removeSession(clientId);
            log.info("Removed session for client {}", clientId);
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("WebSocket transport error for session {}: {}", session.getId(), exception.getMessage());

        Long clientId = sessionToClientId.remove(session.getId());
        if (clientId != null) {
            sessionManager.removeSession(clientId);
        }
    }
}

