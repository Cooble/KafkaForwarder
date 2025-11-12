package com.example.forwarder.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "externalData")
public record ExternalData() { }