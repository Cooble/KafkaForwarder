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

import java.time.LocalDate;

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
        sendMessage(buildDocument(count));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void sendBurstOnStartup() {
        if (!runBurstOnStartup || burstCount <= 0) {
            return;
        }

        log.info("Sending burst of {} events at max speed", burstCount);

        for (int i = 0; i < burstCount; i++) {
            count++;
            sendMessage(buildDocument(count));
        }
        
        log.info("Burst send completed");
    }

    public void sendMessage(InternalData data) {
        log.info("Sending message: {}", data);
        kafkaTemplate.send("topic1", data);
    }

    private InternalData buildDocument(int sequence) {
        final String documentId = "DOC-" + sequence;
        final String customerId = "CUST-" + (sequence % 1000);
        final String currency = "USD";
        final long totalCents = 10_000L + (sequence % 5_000);
        final String payloadJson = generatePayloadJson(documentId, customerId, currency, totalCents);

        return new InternalData(documentId, customerId, currency, totalCents, payloadJson);
    }

    private String generatePayloadJson(String documentId, String customerId, String currency, long totalCents) {
        final var filler = new StringBuilder();
        final String unit = "line-item-detail-1234567890;";
        while (filler.length() < 700) {
            filler.append(unit);
        }
        final String notes = filler.substring(0, 700);

        return """
                {
                  "documentId": "%s",
                  "issueDate": "%s",
                  "dueDate": "%s",
                  "customerId": "%s",
                  "currency": "%s",
                  "totalCents": %d,
                  "lines": [
                    {"sku": "SKU-001", "qty": 2, "priceCents": 1999, "description": "Subscription"},
                    {"sku": "SKU-002", "qty": 1, "priceCents": 4999, "description": "Service fee"},
                    {"sku": "SKU-003", "qty": 3, "priceCents": 1299, "description": "Addon pack"}
                  ],
                  "notes": "%s"
                }
                """.formatted(
                documentId,
                LocalDate.now(),
                LocalDate.now().plusDays(30),
                customerId,
                currency,
                totalCents,
                notes
        );
    }
}
