package com.example.forwarder.model;

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
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "client_identifier")
    private String clientIdentifier;

    @Column(name = "client_url")
    private String clientUrl;

    @ElementCollection(fetch = FetchType.EAGER)
    private List<String> subscribedTopics;

    public Client() {
    }

    public Client(String clientIdentifier, String clientUrl, List<String> subscribedTopics) {
        this.clientIdentifier = clientIdentifier;
        this.clientUrl = clientUrl;
        this.subscribedTopics = subscribedTopics;
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

    public String getClientUrl() {
        return clientUrl;
    }

    public void setClientUrl(String clientUrl) {
        this.clientUrl = clientUrl;
    }

    public List<String> getSubscribedTopics() {
        return subscribedTopics;
    }

    public void setSubscribedTopics(List<String> subscribedTopics) {
        this.subscribedTopics = subscribedTopics;
    }
}