package com.example.forwarder.model;

import jakarta.persistence.*;

import java.util.ArrayList;
import java.util.List;


@Entity
public class Client {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private String clientIdentifier;
    private String clientUrl;
    @ElementCollection
    private List<String> subscribedTopics;

    // One Client -> many external data entries
    @OneToMany(mappedBy = "client", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<ExternalData> externalDataList = new ArrayList<>();

    public Client() {}

    public Client(String clientIdentifier, String clientUrl, List<String> subscribedTopics) {
        this.clientIdentifier = clientIdentifier;
        this.clientUrl = clientUrl;
        this.subscribedTopics = subscribedTopics;
    }
}