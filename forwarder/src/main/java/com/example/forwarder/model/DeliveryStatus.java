package com.example.forwarder.model;

import jakarta.persistence.*;

@Entity
@Table(
        uniqueConstraints = {
                @UniqueConstraint(columnNames = {"external_data_id", "client_id"})
        },
        indexes = {
                @Index(name = "idx_delivery_confirmed", columnList = "confirmed"),
                @Index(name = "idx_delivery_external_data", columnList = "external_data_id")
        }
)
public class DeliveryStatus {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "external_data_id", nullable = false)
    private Long externalDataId;

    @Column(name = "client_id", nullable = false)
    private Long clientId;

    @Column(name = "confirmed", nullable = false)
    private boolean confirmed = false;

    @Column(name = "born_time_ms", nullable = false)
    private long bornTimeMs;  // Timestamp when event was received from Kafka (for metrics)

    public DeliveryStatus() {}

    public DeliveryStatus(Long externalDataId, Long clientId) {
        this.externalDataId = externalDataId;
        this.clientId = clientId;
        this.bornTimeMs = System.currentTimeMillis();
    }

    public DeliveryStatus(Long externalDataId, Long clientId, long bornTimeMs) {
        this.externalDataId = externalDataId;
        this.clientId = clientId;
        this.bornTimeMs = bornTimeMs;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getExternalDataId() {
        return externalDataId;
    }

    public void setExternalDataId(Long externalDataId) {
        this.externalDataId = externalDataId;
    }

    public Long getClientId() {
        return clientId;
    }

    public void setClientId(Long clientId) {
        this.clientId = clientId;
    }

    public boolean isConfirmed() {
        return confirmed;
    }

    public void setConfirmed(boolean confirmed) {
        this.confirmed = confirmed;
    }


    public long getBornTimeMs() {
        return bornTimeMs;
    }

    public void setBornTimeMs(long bornTimeMs) {
        this.bornTimeMs = bornTimeMs;
    }
}

