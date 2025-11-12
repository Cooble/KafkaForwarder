package com.example.forwarder.model;

import jakarta.persistence.*;


@Entity
public class ExternalData {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    // Many ExternalData -> One Client
    @ManyToOne
    @JoinColumn(name = "client_id", nullable = false)
    private Client client;

    private String msg;
    private String name;
    private String externalNew;

    public ExternalData() {
    }

    public ExternalData(Client client, String msg, String name, String externalNew) {
        this.client = client;
        this.msg = msg;
        this.name = name;
        this.externalNew = externalNew;
    }
}