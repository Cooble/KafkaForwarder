package com.example.forwarder;

import com.example.forwarder.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

// Legacy confirmation endpoint for clients to confirm receipt of forwarded data, now handled via response code
@RestController
public class ConfirmationController {
    @Autowired
    private DbService dbService;

    private static final Logger log = LoggerFactory.getLogger(ConfirmationController.class);

    @PostMapping("/confirm")
    public ResponseEntity<Map<String, String>> confirmDelivery(@RequestBody ConfirmationRequest request) {
        log.info("Received confirmation for data ID {} from client {}",
                request.dataId(), request.clientIdentifier());

        boolean success = dbService.markAsConfirmed(request.dataId(), request.clientIdentifier());

        if (success) {
            log.info("Successfully confirmed delivery of data {} to client {}",
                    request.dataId(), request.clientIdentifier());
            return ResponseEntity.ok(Map.of("status", "confirmed"));
        } else {
            log.warn("Failed to confirm delivery - data or client not found: data={}, client={}",
                    request.dataId(), request.clientIdentifier());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("status", "not_found", "message", "Data or client not found"));
        }
    }

    public record ConfirmationRequest(Long dataId, String clientIdentifier) {}
}

