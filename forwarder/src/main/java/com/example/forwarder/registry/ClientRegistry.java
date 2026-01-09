package com.example.forwarder.registry;

import com.example.forwarder.db.DbService;
import com.example.forwarder.model.Client;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * In-memory client registry with immutable snapshots for lock-free reads.
 *
 * Optimized for high read frequency (2000/sec) with rare writes (~once per 10min).
 * Uses volatile references to immutable maps - reads are completely lock-free.
 * Writes rebuild the entire index (fine since they're rare).
 *
 * DB serves as durable storage - loaded on startup, updated on registration.
 */
@Service
public class ClientRegistry {
    private static final Logger log = LoggerFactory.getLogger(ClientRegistry.class);

    @Autowired
    private DbService dbService;

    // Immutable snapshots - volatile ensures visibility across threads
    // Reads are 100% lock-free (just a volatile read)
    private volatile Map<Long, Client> clientsById = Map.of();
    private volatile Map<String, List<Client>> clientsByTopic = Map.of();

    /**
     * Load all clients from DB on startup and build the index.
     */
    @PostConstruct
    public void init() {
        log.info("Initializing ClientRegistry - loading clients from database...");
        List<Client> clients = dbService.getAllClients();
        rebuildIndex(clients);
        log.info("ClientRegistry initialized with {} clients", clients.size());
    }

    /**
     * Get clients for multiple topics - the HOT PATH (2000 calls/sec).
     * Completely lock-free - just volatile reads and map lookups.
     */
    public Map<String, List<Client>> getClientsByTopics(List<String> topics) {
        Map<String, List<Client>> snapshot = this.clientsByTopic; // grab reference once

        Map<String, List<Client>> result = new HashMap<>();
        for (String topic : topics) {
            result.put(topic, snapshot.getOrDefault(topic, List.of()));
        }
        return result;
    }

    /**
     * Get clients for a single topic - lock-free O(1) lookup.
     */
    public List<Client> getClientsByTopic(String topic) {
        return clientsByTopic.getOrDefault(topic, List.of());
    }

    /**
     * Get client by ID - lock-free O(1) lookup.
     */
    public Optional<Client> getClientById(Long id) {
        return Optional.ofNullable(clientsById.get(id));
    }

    /**
     * Get all clients - returns immutable snapshot.
     */
    public Collection<Client> getAllClients() {
        return clientsById.values();
    }

    /**
     * Register or update a client - the SLOW PATH (once per 10min).
     * Persists to DB first, then rebuilds the entire in-memory index.
     * Synchronized since writes are rare and we want atomicity.
     */
    public synchronized Client registerClient(Client client) {
        // 1. Persist to DB (source of truth for durability)
        Client savedClient = dbService.upsertClient(client);

        // 2. Rebuild the entire index with the new/updated client
        List<Client> allClients = new ArrayList<>(clientsById.values());

        // Remove old version if exists (by ID), add new version
        allClients.removeIf(c -> c.getId().equals(savedClient.getId()));
        allClients.add(savedClient);

        rebuildIndex(allClients);

        log.info("Client registered/updated: {} (ID: {}), subscribed to topics: {}",
                savedClient.getClientIdentifier(), savedClient.getId(), savedClient.getSubscribedTopics());

        return savedClient;
    }

    /**
     * Rebuild the immutable index from a list of clients.
     * Creates new immutable maps and atomically swaps the references.
     */
    private void rebuildIndex(List<Client> clients) {
        // Build new clientsById map
        Map<Long, Client> newClientsById = new HashMap<>();
        for (Client client : clients) {
            newClientsById.put(client.getId(), client);
        }

        // Build new topic index
        Map<String, List<Client>> newTopicIndex = new HashMap<>();
        for (Client client : clients) {
            for (String topic : client.getSubscribedTopics()) {
                newTopicIndex.computeIfAbsent(topic, k -> new ArrayList<>()).add(client);
            }
        }

        // Make lists immutable
        for (Map.Entry<String, List<Client>> entry : newTopicIndex.entrySet()) {
            newTopicIndex.put(entry.getKey(), List.copyOf(entry.getValue()));
        }

        // Atomic swap - readers will see either old or new, never a mix
        this.clientsById = Map.copyOf(newClientsById);
        this.clientsByTopic = Map.copyOf(newTopicIndex);

        log.debug("Client index rebuilt: {} clients, {} topics",
                newClientsById.size(), newTopicIndex.size());
    }
}

