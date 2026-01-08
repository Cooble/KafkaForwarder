package com.example.producer;

import com.example.common.InternalData;

import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;

public class EventFactory {

    /**
     * Generates an accounting-style document payload (~1 KB) with IDs and totals.
     */
    public static InternalData createNaturalEvent(long sequenceId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        String eventId = System.currentTimeMillis() + "-" + sequenceId;
        String documentId = "DOC-" + sequenceId;
        String customerId = "CUST-" + (sequenceId % 5000);
        String currency = "USD";
        long totalCents = 10_000L + random.nextLong(0, 10_000L);

        // Build a padded notes field to target ~1KB total payload
        StringBuilder filler = new StringBuilder();
        String unit = "line-item-detail-1234567890;";
        while (filler.length() < 700) {
            filler.append(unit);
        }
        String notes = filler.substring(0, 700);

        String payloadJson = """
                {
                  "eventId": "%s",
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
                eventId,
                documentId,
                LocalDate.now(),
                LocalDate.now().plusDays(30),
                customerId,
                currency,
                totalCents,
                notes
        );

        return new InternalData(
                eventId,
                documentId,
                customerId,
                currency,
                totalCents,
                payloadJson
        );
    }
}