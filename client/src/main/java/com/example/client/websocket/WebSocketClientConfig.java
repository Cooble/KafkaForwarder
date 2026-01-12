package com.example.client.websocket;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.WebSocketContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

/**
 * WebSocket client configuration for larger message buffers.
 */
@Configuration
@ConditionalOnProperty(name = "client.transport.mode", havingValue = "websocket")
public class WebSocketClientConfig {

    @Bean
    public StandardWebSocketClient webSocketClient() {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        container.setDefaultMaxTextMessageBufferSize(2097152); // 2MB
        container.setDefaultMaxBinaryMessageBufferSize(2097152); // 2MB

        StandardWebSocketClient client = new StandardWebSocketClient(container);
        return client;
    }

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(2097152); // 2MB
        container.setMaxBinaryMessageBufferSize(2097152); // 2MB
        container.setMaxSessionIdleTimeout(300000L); // 5 minutes
        return container;
    }
}

