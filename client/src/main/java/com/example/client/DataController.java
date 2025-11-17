package com.example.client;

import com.example.common.ExternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class DataController {
    private static final Logger log = LoggerFactory.getLogger(DataController.class);

    @PostMapping("/data")
    public ResponseEntity<String> receiveData(@RequestBody ExternalData data) {
        log.info("Received data: msg={}, name={}, externalNew={}",
                data.msg(), data.name(), data.externalNew());

        // Process the data
        log.info("Processing data: {}", data.name());

        // Return 200 OK = confirmation of receipt
        // No need to make another HTTP call back!
        return ResponseEntity.ok("Data received");
    }
}

