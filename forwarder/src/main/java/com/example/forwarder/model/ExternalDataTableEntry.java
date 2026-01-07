package com.example.forwarder.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(indexes = {
        @Index(name = "idx_external_data_sequence", columnList = "sequence_number"),
        @Index(name = "idx_external_data_topic", columnList = "topic"),
        @Index(name = "idx_external_data_doc", columnList = "document_id")
})
public class ExternalDataTableEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "topic")
    private String topic;

    @Column(name = "document_id")
    private String documentId;

    @Column(name = "customer_id")
    private String customerId;

    @Column(name = "currency")
    private String currency;

    @Column(name = "total_cents")
    private Long totalCents;

    @Lob
    @Column(name = "payload_json")
    private String payloadJson;

    @Column(name = "sequence_number")
    private Long sequenceNumber;  // Kafka offset - maintains message order

    @Column(name = "received_at")
    private LocalDateTime receivedAt;

    public ExternalDataTableEntry() {
        this.receivedAt = LocalDateTime.now();
    }

    public ExternalDataTableEntry(
            String topic,
            String documentId,
            String customerId,
            String currency,
            Long totalCents,
            String payloadJson,
            Long sequenceNumber
    ) {
        this.topic = topic;
        this.documentId = documentId;
        this.customerId = customerId;
        this.currency = currency;
        this.totalCents = totalCents;
        this.payloadJson = payloadJson;
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

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Long getTotalCents() {
        return totalCents;
    }

    public void setTotalCents(Long totalCents) {
        this.totalCents = totalCents;
    }

    public String getPayloadJson() {
        return payloadJson;
    }

    public void setPayloadJson(String payloadJson) {
        this.payloadJson = payloadJson;
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

