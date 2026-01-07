package com.example.client;

import com.example.common.ExternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class DataController {
    private static final Logger log = LoggerFactory.getLogger(DataController.class);

    private final AtomicLong receivedCount = new AtomicLong(0);

    @PostMapping("/data")
    public ResponseEntity<String> receiveData(@RequestBody ExternalData data) {
        receivedCount.incrementAndGet();

        // Process the data silently
        // Return 200 OK = confirmation of receipt
        return ResponseEntity.ok("Data received");
    }

    @PostMapping("/data/batch")
    public ResponseEntity<String> receiveDataBatch(@RequestBody List<ExternalData> dataList) {
        receivedCount.addAndGet(dataList.size());

        // Process the batch silently
        // Return 200 OK = confirmation of receipt for all events
        return ResponseEntity.ok("Batch received: " + dataList.size() + " events");
    }

    @Scheduled(fixedRate = 1000)
    public void logStats() {
        long received = receivedCount.getAndSet(0);
       // if (received > 0) {
            log.info("Client: Acknowledged {} events/sec", received);
      //  }
    }
}

