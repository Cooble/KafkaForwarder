package com.example.producer;

import com.example.common.InternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class PublishService {
    @Autowired
    private KafkaTemplate<String, InternalData> kafkaTemplate;
    private static final Logger log = LoggerFactory.getLogger(PublishService.class);

    @Value("${producer.scheduler.enabled:true}")
    private boolean schedulerEnabled;

    @Value("${producer.burst.count:0}")
    private int burstCount;

    @Value("${producer.burst.run-on-startup:true}")
    private boolean runBurstOnStartup;

    private int count = 0;

    @Scheduled(fixedRateString = "${producer.rate.period.ms}")
    public void scheduledEvent() {
        if (!schedulerEnabled) {
            return;
        }

        count++;
        sendMessage(new InternalData("test", "name" + count));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void sendBurstOnStartup() {
        if (!runBurstOnStartup || burstCount <= 0) {
            return;
        }

        log.info("Sending burst of {} events at max speed", burstCount);

        for (int i = 0; i < burstCount; i++) {
            count++;
            sendMessage(new InternalData("test", "name" + count));
        }
        
        log.info("Burst send completed");
    }

    public void sendMessage(InternalData data) {
        log.info("Sending message: {}", data);
        kafkaTemplate.send("topic1", data);
    }
}
