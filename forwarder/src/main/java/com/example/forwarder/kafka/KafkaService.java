package com.example.forwarder.kafka;

import com.example.forwarder.TransformationService;
import com.example.common.InternalData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    @Autowired
    private TransformationService transformationService;

    @KafkaListener(topics = "${kafka.topics}")
    public void listenInternalData(InternalData message) {
         System.out.println("Received message: " + message);
    }
}
