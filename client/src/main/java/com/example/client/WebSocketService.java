package com.example.client;

import com.example.common.ConfirmationRequest;
import com.example.common.ExternalData;
import com.example.common.RegistrationRequest;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class WebSocketService {
    private static final Logger log = LoggerFactory.getLogger(WebSocketService.class);

    @Value("${forwarder.url}")
    private String forwarderUrl;

    @Value("${client.subscribed.topics}")
    private String subscribedTopicsStr;

    @Value("${client.identifier:unknown}")
    private String clientIdentifier;

    @Value("${client.processing.delay.ms:0}")
    private long processingDelayMs;

    @Value("${client.processing.delay.jitter.ms:0}")
    private long processingDelayJitterMs;

    @Value("${client.processing.threads:4}")
    private int processingThreads;

    private WebSocketStompClient stompClient;
    private StompSession stompSession;
    private ExecutorService processingExecutor;

    @PostConstruct
    public void connect() {
        processingExecutor = Executors.newCachedThreadPool();

        List<Transport> transports = Arrays.asList(new WebSocketTransport(new StandardWebSocketClient()));
        SockJsClient sockJsClient = new SockJsClient(transports);

        stompClient = new WebSocketStompClient(sockJsClient);
        stompClient.setMessageConverter(new MappingJackson2MessageConverter());

        try {
            stompSession = stompClient.connectAsync(forwarderUrl, new StompSessionHandlerAdapter() {
                @Override
                public void afterConnected(final StompSession session, final StompHeaders connectedHeaders) {
                    log.info("Connected to WebSocket at {}", forwarderUrl);

                    final var topics = Arrays
                            .stream(subscribedTopicsStr.split(","))
                            .peek(topic -> {
                                session.subscribe("/topic/" + topic, this);
                                log.info("Subscribed to topic: {}", topic);
                            })
                            .toList();
                    
                    // Subscribe to user queue for retries
                    session.subscribe("/queue/data/" + clientIdentifier, this);

                    final var regRequest = new RegistrationRequest(clientIdentifier, topics);
                    
                    session.send("/app/register", regRequest);
                    log.info("Sent registration for topics: {}", topics);
                }

                @Override
                public void handleFrame(final StompHeaders headers, final Object payload) {
                    if (payload instanceof ExternalData data) {
                        processingExecutor.submit(() -> processData(data));
                    }
                }

                @Override
                public Type getPayloadType(StompHeaders headers) {
                    return ExternalData.class;
                }

                @Override
                public void handleException(StompSession session, StompCommand command, StompHeaders headers, byte[] payload, Throwable exception) {
                    log.error("WebSocket error: ", exception);
                }

                @Override
                public void handleTransportError(StompSession session, Throwable exception) {
                    log.error("WebSocket transport error: ", exception);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to connect to WebSocket", e);
        }
    }

    private void processData(final ExternalData data) {
        log.info(
            "Received doc: id={}, docId={}, customer={}, currency={}, totalCents={}, payloadSize={}B",
            data.id(),
            data.documentId(),
            data.customerId(),
            data.currency(),
            data.totalCents(),
            data.payloadJson() != null ? data.payloadJson().length() : 0
        );

        if (processingDelayMs > 0 || processingDelayJitterMs > 0) {
            long delay = processingDelayMs;

            if (processingDelayJitterMs > 0) {
                delay += java.util.concurrent.ThreadLocalRandom.current().nextLong(0, processingDelayJitterMs + 1);
            }

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Processing delay interrupted", e);
            }
        }

        final var confirmRequest = new ConfirmationRequest(data.id(), clientIdentifier);

        stompSession.send("/app/confirm", confirmRequest);
        log.info("Sent confirmation for data ID {}", data.id());
    }

    @PreDestroy
    public void shutdownExecutor() {
        if (processingExecutor != null) {
            processingExecutor.shutdown();
        }
    }
}
