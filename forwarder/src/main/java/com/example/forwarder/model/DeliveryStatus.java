package com.example.forwarder.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(
        uniqueConstraints = {
                @UniqueConstraint(columnNames = {"external_data_id", "client_id"})
        },
        indexes = {
                @Index(name = "idx_delivery_last_attempt", columnList = "last_attempt, confirmed"),
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

    @Column(name = "last_attempt")
    private LocalDateTime lastAttempt;  // For retry timing - functionally necessary

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount = 0;

    public DeliveryStatus() {}

    public DeliveryStatus(Long externalDataId, Long clientId) {
        this.externalDataId = externalDataId;
        this.clientId = clientId;
        this.lastAttempt = LocalDateTime.now();
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

    public LocalDateTime getLastAttempt() {
        return lastAttempt;
    }

    public void setLastAttempt(LocalDateTime lastAttempt) {
        this.lastAttempt = lastAttempt;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public void incrementAttemptCount() {
        this.attemptCount++;
    }
}

