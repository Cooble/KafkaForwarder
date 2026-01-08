package com.example.client.websocket;

import com.example.common.AckMessage;
import com.example.common.DataMessage;
import com.example.common.RegistrationRequest;
import com.example.common.WebSocketMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
@ConditionalOnProperty(name = "client.transport.mode", havingValue = "websocket")
public class WebSocketClientService extends TextWebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(WebSocketClientService.class);

    @Value("${forwarder.websocket.url}")
    private String forwarderWebSocketUrl;

    @Value("${client.subscribed.topics}")
    private String subscribedTopicsStr;

    @Value("${client.websocket.reconnect.max.attempts:10}")
    private int maxReconnectAttempts;

    @Value("${client.websocket.reconnect.delay.ms:2000}")
    private long reconnectDelayMs;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private WebSocketClient webSocketClient;

    private WebSocketSession session;
    private volatile boolean shouldReconnect = true;

    private final AtomicLong totalEventsReceived = new AtomicLong(0);
    private final AtomicLong eventsReceivedSinceLastLog = new AtomicLong(0);

    @PostConstruct
    public void connectOnStartup() {
        new Thread(this::connectWithRetry).start();
    }

    @PreDestroy
    public void disconnect() {
        shouldReconnect = false;
        if (session != null && session.isOpen()) {
            try {
                session.close();
            } catch (Exception e) {
                log.error("Error closing WebSocket session: {}", e.getMessage());
            }
        }
    }

    private void connectWithRetry() {
        int attempt = 0;
        while (shouldReconnect && attempt < maxReconnectAttempts) {
            attempt++;
            try {
                log.info("Connecting to forwarder WebSocket (attempt {}/{}): {}",
                    attempt, maxReconnectAttempts, forwarderWebSocketUrl);

                session = webSocketClient.execute(this, forwarderWebSocketUrl).get();

                log.info("Connected to forwarder WebSocket");

                sendRegistration();

                return;

            } catch (Exception e) {
                log.warn("Failed to connect to forwarder (attempt {}/{}): {}",
                    attempt, maxReconnectAttempts, e.getMessage());

                if (attempt < maxReconnectAttempts) {
                    try {
                        Thread.sleep(reconnectDelayMs * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        log.error("Failed to connect to forwarder after {} attempts", maxReconnectAttempts);
    }

    private void sendRegistration() throws Exception {
        List<String> topics = Arrays.asList(subscribedTopicsStr.split(","));
        RegistrationRequest registration = new RegistrationRequest("", topics);

        WebSocketMessage wsMessage = new WebSocketMessage(
            WebSocketMessage.MessageType.REGISTER,
            registration
        );

        String jsonMessage = objectMapper.writeValueAsString(wsMessage);
        session.sendMessage(new TextMessage(jsonMessage));

        log.info("Sent registration for topics: {}", topics);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        try {
            WebSocketMessage wsMessage = objectMapper.readValue(message.getPayload(), WebSocketMessage.class);

            switch (wsMessage.type()) {
                case DATA:
                    handleDataMessage(wsMessage);
                    break;
                case PONG:
                    break;
                default:
                    log.warn("Unknown message type: {}", wsMessage.type());
            }
        } catch (Exception e) {
            log.error("Error handling WebSocket message: {}", e.getMessage(), e);
        }
    }

    private void handleDataMessage(WebSocketMessage wsMessage) throws Exception {
        DataMessage dataMessage = objectMapper.convertValue(wsMessage.payload(), DataMessage.class);

        int count = dataMessage.data().size();
        totalEventsReceived.addAndGet(count);
        eventsReceivedSinceLastLog.addAndGet(count);

        log.debug("Received batch of {} events", count);

        sendAcknowledgment(dataMessage.dataIds());
    }

    private void sendAcknowledgment(List<Long> dataIds) throws Exception {
        AckMessage ackMessage = new AckMessage(dataIds);
        WebSocketMessage wsMessage = new WebSocketMessage(
            WebSocketMessage.MessageType.ACK,
            ackMessage
        );

        String jsonMessage = objectMapper.writeValueAsString(wsMessage);
        session.sendMessage(new TextMessage(jsonMessage));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        log.warn("WebSocket connection closed: {}", status);

        if (shouldReconnect) {
            log.info("Attempting to reconnect...");
            new Thread(this::connectWithRetry).start();
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("WebSocket transport error: {}", exception.getMessage());
    }

    @Scheduled(fixedRate = 1000)
    public void logStats() {
        long eventsSinceLastLog = eventsReceivedSinceLastLog.getAndSet(0);
        long total = totalEventsReceived.get();

        log.info("Received {} events in last second | Total: {} events",
                    eventsSinceLastLog, total);

    }

    @Scheduled(fixedRate = 30000)
    public void sendHeartbeat() {
        if (session != null && session.isOpen()) {
            try {
                WebSocketMessage pingMessage = new WebSocketMessage(WebSocketMessage.MessageType.PING, null);
                String jsonMessage = objectMapper.writeValueAsString(pingMessage);
                session.sendMessage(new TextMessage(jsonMessage));
                log.debug("Sent heartbeat ping");
            } catch (Exception e) {
                log.warn("Failed to send heartbeat: {}", e.getMessage());
            }
        }
    }
}

