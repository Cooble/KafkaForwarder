package com.example.forwarder.model;

import com.example.common.RegistrationRequest;
import jakarta.persistence.*;

import java.util.List;

@Entity
@Table(
        uniqueConstraints = {
                @UniqueConstraint(columnNames = "client_identifier")
        },
        indexes = {
                @Index(name = "idx_client_identifier", columnList = "client_identifier")
        }
)
public class Client {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "client_identifier")
    private String clientIdentifier;

    @ElementCollection(fetch = FetchType.EAGER)
    private List<String> subscribedTopics;

    // for Hibernate
    public Client() {}

    public Client(final RegistrationRequest request) {
        clientIdentifier = request.clientIdentifier();
        subscribedTopics = request.topics();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getClientIdentifier() {
        return clientIdentifier;
    }

    public void setClientIdentifier(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
    }

    public List<String> getSubscribedTopics() {
        return subscribedTopics;
    }

    public void setSubscribedTopics(List<String> subscribedTopics) {
        this.subscribedTopics = subscribedTopics;
    }
}