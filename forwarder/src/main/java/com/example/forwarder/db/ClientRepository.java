package com.example.forwarder.db;

import com.example.forwarder.model.Client;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ClientRepository extends JpaRepository<Client, Long> {
    Client findByClientIdentifier(String clientIdentifier);
    Client findBySessionId(String sessionId);
}