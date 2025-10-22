package com.example.client;

import com.example.common.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.server.WebSocketServer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExternalClient {
    public static void main(String[] args) throws Exception {
        int port = 8081;
        ObjectMapper mapper = new ObjectMapper();

        WebSocketServer server = new WebSocketServer(new InetSocketAddress(port)) {
            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                System.out.println("Client WS opened: " + conn.getRemoteSocketAddress());
            }

            @Override
            public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                System.out.println("Client WS closed: " + reason);
            }

            @Override
            public void onMessage(WebSocket conn, String message) {
                try {
                    Event ev = mapper.readValue(message, Event.class);
                    System.out.println("External client received event: " + ev);
                    // simulate processing then send ACK with id
                    Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                        try {
                            String ack = mapper.createObjectNode()
                                    .put("id", ev.getId())
                                    .put("status", "ACK")
                                    .toString();
                            conn.send(ack);
                            System.out.println("Sent ACK for: " + ev.getId());
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }, 500, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    System.err.println("Failed to parse event: " + e.getMessage());
                }
            }

            @Override
            public void onError(WebSocket conn, Exception ex) {
                System.err.println("WS error: " + ex.getMessage());
            }

            @Override
            public void onStart() {
                System.out.println("External WebSocket server started on port " + port);
            }
        };

        server.start();
        System.out.println("External client running. Press Ctrl+C to stop.");
    }
}
