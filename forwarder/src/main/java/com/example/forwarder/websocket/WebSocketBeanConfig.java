package com.example.forwarder.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "forwarder.transport.mode", havingValue = "websocket")
public class WebSocketBeanConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

