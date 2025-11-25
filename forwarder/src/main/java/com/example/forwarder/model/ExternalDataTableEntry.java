package com.example.forwarder.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(indexes = {
        @Index(name = "idx_external_data_sequence", columnList = "sequence_number"),
        @Index(name = "idx_external_data_topic", columnList = "topic")
})
public class ExternalDataTableEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "topic")
    private String topic;

    @Column(name = "msg")
    private String msg;

    @Column(name = "name")
    private String name;

    @Column(name = "external_new")
    private String externalNew;

    @Column(name = "sequence_number")
    private Long sequenceNumber;  // Kafka offset - maintains message order

    @Column(name = "received_at")
    private LocalDateTime receivedAt;

    public ExternalDataTableEntry() {
        this.receivedAt = LocalDateTime.now();
    }

    public ExternalDataTableEntry(String topic, String msg, String name, String externalNew, Long sequenceNumber) {
        this.topic = topic;
        this.msg = msg;
        this.name = name;
        this.externalNew = externalNew;
        this.sequenceNumber = sequenceNumber;
        this.receivedAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExternalNew() {
        return externalNew;
    }

    public void setExternalNew(String externalNew) {
        this.externalNew = externalNew;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(LocalDateTime receivedAt) {
        this.receivedAt = receivedAt;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
}

