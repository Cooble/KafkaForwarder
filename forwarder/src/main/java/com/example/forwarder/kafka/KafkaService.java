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

         /*
         * Option 1 - call Send Service at this point (or transformation service which then calls send service)
         *          - as soon as event data arrives in send service, save it into DB with the client URL / db primary key ID (maybe change it to UUID?)?
         *          - once it is sent to this client, remove the DB entry
         *
         * Option 2 - as soon as InternalData is received, save in into DB
         *          - then pass internal data to Transformation Service / Send service
         *          - Once send service sends the transformed data to a client, it adds a record into DB indicating that this client has the data
         *          - Once data has been sent to all clients remove the InternalData DB entry, together with all record of clients which have received this data (SQL Cascade delete?)
         *
         * Option 2 can perhaps guarantee better protection against data loss
         *
         * */
    }
}
