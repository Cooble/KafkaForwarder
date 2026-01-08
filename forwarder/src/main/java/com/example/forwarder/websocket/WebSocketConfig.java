package com.example.forwarder.websocket;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
@ConditionalOnProperty(name = "forwarder.transport.mode", havingValue = "websocket")
public class WebSocketConfig implements WebSocketConfigurer {

    @Value("${forwarder.websocket.endpoint:/ws}")
    private String websocketEndpoint;

    @Autowired
    private ForwarderWebSocketHandler webSocketHandler;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(webSocketHandler, websocketEndpoint)
                .setAllowedOrigins("*");
    }
}

