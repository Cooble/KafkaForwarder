package com.example.common;

public record WebSocketMessage(MessageType type, Object payload) {

    public enum MessageType {
        REGISTER,       // Client registration
        DATA,           // Data delivery (single or batch)
        ACK,            // Acknowledgment
        PING,           // Heartbeat ping
        PONG            // Heartbeat pong
    }
}

