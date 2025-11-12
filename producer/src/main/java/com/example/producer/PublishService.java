package com.example.producer;

import com.example.common.InternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class PublishService {
    @Autowired
    private KafkaTemplate<String, InternalData> kafkaTemplate;
    private static final Logger log = LoggerFactory.getLogger(PublishService.class);

    private int count = 0;

    @Scheduled(fixedRate = 3000)
    public void scheduledEvent() {
        count++;
        sendMessage(new InternalData("test", "name" + count));
    }

    public void sendMessage(InternalData data) {
        log.info("Sending message: {}", data);
        kafkaTemplate.send("topic1", data);
    }
}
