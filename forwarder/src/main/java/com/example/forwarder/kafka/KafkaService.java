package com.example.forwarder.kafka;

import com.example.forwarder.TransformationService;
import com.example.common.InternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    @Autowired
    private TransformationService transformationService;

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);

    @KafkaListener(topics = "${kafka.topics}")
    public void listenInternalData(InternalData message) {
         log.info("Received event message: {}", message);
    }
}
