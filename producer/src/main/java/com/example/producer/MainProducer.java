package com.example.producer;

import com.example.common.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.UUID;
import java.util.Random;

@SpringBootApplication
public class MainProducer {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(MainProducer.class, args);
        runProducer();
    }

    private static void runProducer() throws Exception {
        String topic = "test-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random rnd = new Random();

        System.out.println("Producer started, producing to topic '" + topic + "' every 2s. Ctrl+C to stop.");
        while (true) {
            Event ev = new Event(UUID.randomUUID().toString(), "payload-" + rnd.nextInt(10000), System.currentTimeMillis());
            String json = mapper.writeValueAsString(ev);
            producer.send(new ProducerRecord<>(topic, ev.getId(), json), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Send failed: " + exception.getMessage());
                } else {
                    System.out.println("Produced: " + json);
                }
            });
            Thread.sleep(2000);
        }
    }
}
