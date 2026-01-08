package com.example.client;

import com.example.common.ExternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@ConditionalOnProperty(name = "client.transport.mode", havingValue = "rest")
public class DataController {
    private static final Logger log = LoggerFactory.getLogger(DataController.class);

    private final AtomicLong totalEventsReceived = new AtomicLong(0);
    private final AtomicLong eventsReceivedSinceLastLog = new AtomicLong(0);
    private final Set<Long> uniqueEventIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @PostMapping("/data")
    public ResponseEntity<String> receiveData(@RequestBody ExternalData data) {
        // Process the data silently
        totalEventsReceived.incrementAndGet();
        eventsReceivedSinceLastLog.incrementAndGet();
        uniqueEventIds.add(data.totalCents());
        // Return 200 OK = confirmation of receipt
        return ResponseEntity.ok("Data received");
    }

    @PostMapping("/data/batch")
    public ResponseEntity<String> receiveDataBatch(@RequestBody List<ExternalData> dataList) {
        // Process the batch silently
        int count = dataList.size();
        totalEventsReceived.addAndGet(count);
        eventsReceivedSinceLastLog.addAndGet(count);
        dataList.forEach(data -> uniqueEventIds.add(data.totalCents()));
        // Return 200 OK = confirmation of receipt for all events
        return ResponseEntity.ok("Batch received: " + count + " events");
    }

    @Scheduled(fixedRate = 1000)
    public void logStats() {
        long eventsSinceLastLog = eventsReceivedSinceLastLog.getAndSet(0);
        long total = totalEventsReceived.get();
        int unique = uniqueEventIds.size();
        long duplicates = total - unique;

        if (eventsSinceLastLog > 0 || total > 0) {
            log.info("Received {} events in last second | Total: {} | Unique: {} | Duplicates: {}",
                    eventsSinceLastLog, total, unique, duplicates);
        }
    }
}
