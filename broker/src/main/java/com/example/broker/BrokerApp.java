package com.example.broker;

import com.example.common.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerApp {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static Connection conn;
    // map of ws connections by URI (single target here)
    private static ConcurrentHashMap<String, WebSocketClient> wsMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        // DB setup
        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/brokerdb", "test", "test");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, payload TEXT, status TEXT)");
        }

        // setup websocket client to external consumer
        String wsUri = "ws://localhost:8081/ws";
        connectWebsocket(wsUri);

        // Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "broker-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("test-topic"));

        System.out.println("Broker started. Listening to Kafka and forwarding over WebSocket.");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : records) {
                String id = r.key();
                String payload = r.value();

                // insert into DB (idempotent)
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO events (id, payload, status) VALUES (?, ?, 'PENDING') ON CONFLICT (id) DO NOTHING")) {
                    ps.setString(1, id);
                    ps.setString(2, payload);
                    ps.executeUpdate();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

                // send over websocket if open
                WebSocketClient ws = wsMap.get(wsUri);
                if (ws != null && ws.isOpen()) {
                    System.out.println("Sending event to external client: " + id);
                    ws.send(payload);
                } else {
                    System.out.println("WebSocket not open, will retry later for: " + id);
                }
            }
            // also poll DB for pending events in case previous sends failed
            retryPending(wsUri);
        }
    }

    private static void retryPending(String wsUri) {
        try (PreparedStatement ps = conn.prepareStatement("SELECT id, payload FROM events WHERE status='PENDING'")) {
            ResultSet rs = ps.executeQuery();
            WebSocketClient ws = wsMap.get(wsUri);
            while (rs.next()) {
                String id = rs.getString("id");
                String payload = rs.getString("payload");
                if (ws != null && ws.isOpen()) {
                    System.out.println("Retrying send for: " + id);
                    ws.send(payload);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    private static void connectWebsocket(String uri) {
        try {
            WebSocketClient client = new WebSocketClient(new URI(uri)) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    System.out.println("WebSocket connected to external client: " + uri);
                }

                @Override
                public void onMessage(String message) {
                    // expecting ACK as JSON like { "id":"...", "status":"ACK" }
                    try {
                        var node = mapper.readTree(message);
                        String id = node.get("id").asText();
                        String status = node.get("status").asText();
                        System.out.println("Received WS message: " + message);
                        if ("ACK".equalsIgnoreCase(status)) {
                            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM events WHERE id = ?")) {
                                ps.setString(1, id);
                                int deleted = ps.executeUpdate();
                                System.out.println("Deleted from DB (ACK): " + id + ", rows=" + deleted);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to parse WS message: " + e.getMessage());
                    }
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    System.out.println("WebSocket closed: " + reason + " — reconnecting in 2s...");
                    wsMap.remove(uri.toString());
                    // simple reconnect
                    new Thread(() -> {
                        try { Thread.sleep(2000); connectWebsocket(uri.toString()); } catch (InterruptedException ignored) {}
                    }).start();
                }

                @Override
                public void onError(Exception ex) {
                    System.err.println("WebSocket error: " + ex.getMessage());
                }
            };
            client.connect();
            wsMap.put(uri, client);
        } catch (Exception e) {
            System.err.println("Failed to create WS client: " + e.getMessage());
        }
    }
}
