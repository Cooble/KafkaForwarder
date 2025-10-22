package com.example.common;

public class Event {
    private String id;
    private String payload;
    private long timestamp;

    public Event() {}

    public Event(String id, String payload, long timestamp) {
        this.id = id;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Event{id='" + id + "', payload='" + payload + "', timestamp=" + timestamp + "}";
    }
}
