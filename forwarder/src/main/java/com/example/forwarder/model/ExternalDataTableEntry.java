package com.example.forwarder.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(
    name = "external_data_table_entry",
    indexes = {
        @Index(name = "idx_external_data_sequence", columnList = "sequence_number"),
        @Index(name = "idx_external_data_topic", columnList = "topic"),
        @Index(name = "idx_external_data_event_id", columnList = "event_id"),
        @Index(name = "idx_external_data_doc", columnList = "document_id")
    },
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_event_id", columnNames = "event_id")
    }
)
public class ExternalDataTableEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", unique = true, nullable = false, length = 100)
    private String eventId;  // Unique event identifier from Kafka message

    @Column(name = "topic")
    private String topic;

    @Column(name = "document_id", length = 200)
    private String documentId;

    @Column(name = "customer_id", length = 200)
    private String customerId;

    @Column(name = "currency", length = 8)
    private String currency;

    @Column(name = "total_cents")
    private Long totalCents;

    @Column(name = "payload_json", columnDefinition = "TEXT")
    private String payloadJson;

    @Column(name = "sequence_number")
    private Long sequenceNumber;  // Kafka offset - maintains message order

    @Column(name = "received_at")
    private LocalDateTime receivedAt;

    public ExternalDataTableEntry() {
        this.receivedAt = LocalDateTime.now();
    }

    public ExternalDataTableEntry(String eventId,
                                  String topic,
                                  String documentId,
                                  String customerId,
                                  String currency,
                                  Long totalCents,
                                  String payloadJson,
                                  Long sequenceNumber) {
        this.eventId = eventId;
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

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
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

