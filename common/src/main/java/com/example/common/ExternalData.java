package com.example.common;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "externalData")
public record ExternalData() { }
