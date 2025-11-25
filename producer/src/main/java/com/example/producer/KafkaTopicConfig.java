package com.example.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {
    
    @Bean
    public NewTopic recordTopic() {
        return new NewTopic("topic1", 1, (short) 1);
    }
}