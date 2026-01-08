package com.example.forwarder.websocket;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.ServletWebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

/**
 * WebSocket container configuration for larger message buffers.
 * This is loaded regardless of transport mode to ensure Tomcat WebSocket container is properly configured.
 */
@Configuration
public class WebSocketContainerConfig {

    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(2097152); // 2MB
        container.setMaxBinaryMessageBufferSize(2097152); // 2MB
        container.setMaxSessionIdleTimeout(300000L); // 5 minutes
        return container;
    }
}

