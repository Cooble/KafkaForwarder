package com.example.broker.kafka;

import com.example.broker.TransformationService;
import com.example.common.InternalData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    @Autowired
    private TransformationService transformationService;

    // TODO move to config
    @KafkaListener(topics = "record")
    public void listenInternalData(InternalData message) {
    }
}
