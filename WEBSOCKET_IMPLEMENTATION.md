# WebSocket vs REST Implementation Guide

## Overview

This project now supports **both REST and WebSocket** communication between the forwarder and clients. You can switch between the two modes using configuration properties to assess performance differences.

## Key Design Principle

**The database is the single source of truth** for pending deliveries. Both REST and WebSocket implementations use the same:
- `ExternalDataTableEntry` - stores events to be delivered
- `DeliveryStatus` - tracks delivery state per (event, client) pair
- `ResendService` - periodically retries failed deliveries

The **only difference** is the transport layer (HTTP vs WebSocket).

---

## Architecture

### Common Components (Both Modes)
- `KafkaService` - Receives events from Kafka, saves to DB, triggers sending
- `ResendService` - Retries failed deliveries by querying DB
- `DeliveryResultHandler` - Handles acknowledgments and updates DB
- `DbService` - Database operations
- `TransformationService` - Transforms events

### REST Mode Components
- `SendService` (implements `MessageSender`) - Sends HTTP POST requests
- `RegistrationController` - Handles client registration via HTTP
- `DataController` (client) - Receives data via HTTP POST
- Acknowledgment: HTTP 200 response = success

### WebSocket Mode Components
- `WebSocketSendService` (implements `MessageSender`) - Sends WebSocket messages
- `WebSocketSessionManager` - Tracks active WebSocket connections
- `ForwarderWebSocketHandler` - Handles WebSocket connections and messages
- `WebSocketClientService` (client) - Maintains WebSocket connection, receives data
- Acknowledgment: Explicit ACK message sent over WebSocket

---

## How to Switch Modes

### Forwarder Configuration

Edit `forwarder/src/main/resources/application.properties`:

```properties
# For REST mode (default)
forwarder.transport.mode=rest

# For WebSocket mode
forwarder.transport.mode=websocket
```

### Client Configuration

Edit `client/src/main/resources/application.properties`:

```properties
# For REST mode (default)
client.transport.mode=rest

# For WebSocket mode
client.transport.mode=websocket
```

**Important:** Both forwarder and client must use the same mode!

---

## Configuration Properties

### REST Mode Properties

**Forwarder:**
```properties
forwarder.transport.mode=rest
forwarder.client.timeout.ms=10000
forwarder.client.max.batch.size=100
forwarder.client.max.concurrent.requests=500
```

**Client:**
```properties
client.transport.mode=rest
forwarder.url=http://localhost:8081
client.url=http://localhost:8082
client.subscribed.topics=topic1
client.auto.register=true
```

### WebSocket Mode Properties

**Forwarder:**
```properties
forwarder.transport.mode=websocket
forwarder.websocket.endpoint=/ws
forwarder.websocket.ack.timeout.ms=10000
forwarder.websocket.heartbeat.interval.ms=30000
forwarder.client.max.batch.size=100
```

**Client:**
```properties
client.transport.mode=websocket
forwarder.websocket.url=ws://localhost:8081/ws
client.subscribed.topics=topic1
client.websocket.reconnect.max.attempts=10
client.websocket.reconnect.delay.ms=2000
```

---

## Message Flow Comparison

### REST Mode Flow

1. **Registration:**
   - Client sends HTTP POST to `/registration` with topics
   - Forwarder stores client info with `clientUrl`

2. **Data Delivery:**
   - Kafka event arrives → saved to DB → `DeliveryStatus` created
   - Forwarder sends HTTP POST to `{clientUrl}/data/batch`
   - Client returns HTTP 200 → `DeliveryStatus.confirmed = true`
   - If failed → `ResendService` retries later

3. **Retry:**
   - `ResendService` queries DB for unconfirmed deliveries
   - Sends HTTP POST again
   - Continues until max attempts reached

### WebSocket Mode Flow

1. **Registration:**
   - Client connects to WebSocket endpoint `/ws`
   - Client sends REGISTER message with topics
   - Forwarder stores client info and WebSocket session

2. **Data Delivery:**
   - Kafka event arrives → saved to DB → `DeliveryStatus` created
   - Forwarder sends DATA message via WebSocket (includes event IDs)
   - Client sends ACK message with event IDs → `DeliveryStatus.confirmed = true`
   - If timeout/no ACK → `ResendService` retries later

3. **Retry:**
   - `ResendService` queries DB for unconfirmed deliveries
   - Sends via WebSocket if session active
   - If session inactive → waits for reconnection

4. **Reconnection:**
   - Client automatically reconnects on disconnect
   - Forwarder re-registers the session
   - Pending deliveries are sent automatically by `ResendService`

---

## WebSocket Message Types

### Message Wrapper
```json
{
  "type": "REGISTER|DATA|ACK|PING|PONG",
  "payload": { ... }
}
```

### REGISTER (Client → Forwarder)
```json
{
  "type": "REGISTER",
  "payload": {
    "clientUrl": "",
    "topics": ["topic1", "topic2"]
  }
}
```

### DATA (Forwarder → Client)
```json
{
  "type": "DATA",
  "payload": {
    "dataIds": [123, 124, 125],
    "data": [
      {"msg": "...", "name": "...", "externalNew": "..."},
      {"msg": "...", "name": "...", "externalNew": "..."}
    ]
  }
}
```

### ACK (Client → Forwarder)
```json
{
  "type": "ACK",
  "payload": {
    "dataIds": [123, 124, 125]
  }
}
```

### PING/PONG (Heartbeat)
```json
{
  "type": "PING",
  "payload": null
}
```

---

## Key Differences

| Aspect | REST | WebSocket |
|--------|------|-----------|
| Connection | Request per message | Persistent connection |
| Client Discovery | `clientUrl` in DB | Active WebSocket session |
| Acknowledgment | HTTP 200 response | Explicit ACK message |
| Retry Mechanism | HTTP POST with timeout | WebSocket send or wait for reconnect |
| Overhead | HTTP headers per request | Minimal frame overhead |
| Stateful/Stateless | Stateless | Stateful (session tracking) |
| Scalability | Horizontal scaling easy | Needs session affinity |
| Reconnection | N/A (stateless) | Automatic with client retry |

---

## Performance Testing

To compare performance:

1. **Start with REST mode:**
   ```
   forwarder.transport.mode=rest
   client.transport.mode=rest
   ```
   - Build and run: `mvn clean package`
   - Monitor metrics in logs

2. **Switch to WebSocket mode:**
   ```
   forwarder.transport.mode=websocket
   client.transport.mode=websocket
   ```
   - Rebuild and run
   - Compare metrics

3. **Metrics to monitor:**
   - Events/sec throughput
   - Latency (born time to ACK time)
   - CPU/memory usage
   - Network bandwidth
   - Connection overhead

---

## Implementation Details

### Abstraction Layer

Both modes implement the `MessageSender` interface:

```java
public interface MessageSender {
    CompletableFuture<SendResult> sendToClient(ExternalDataTableEntry data, Client client);
    CompletableFuture<BatchSendResult> sendBatchToClient(List<ExternalDataTableEntry> dataList, Client client);
    boolean hasCapacity();
    int getActiveRequests();
    int getAvailableCapacity();
}
```

This allows `KafkaService` and `ResendService` to work with either implementation without knowing which transport is used.

### Conditional Bean Loading

Spring's `@ConditionalOnProperty` automatically loads the correct implementation:

```java
@Service
@ConditionalOnProperty(name = "forwarder.transport.mode", havingValue = "rest", matchIfMissing = true)
public class SendService implements MessageSender { ... }

@Service
@ConditionalOnProperty(name = "forwarder.transport.mode", havingValue = "websocket")
public class WebSocketSendService implements MessageSender { ... }
```

Only ONE implementation is loaded at runtime based on configuration.

---

## Building and Running

1. **Compile the project:**
   ```bash
   mvn clean package
   ```

2. **Start Docker services** (Kafka, PostgreSQL):
   ```bash
   docker-compose up -d
   ```

3. **Run forwarder:**
   ```bash
   java -jar forwarder/target/forwarder-1.0-SNAPSHOT.jar
   ```

4. **Run client:**
   ```bash
   java -jar client/target/client-1.0-SNAPSHOT.jar
   ```

5. **Produce test messages** (if you have a producer):
   ```bash
   java -jar producer/target/producer-1.0-SNAPSHOT.jar
   ```

---

## Troubleshooting

### WebSocket Connection Refused
- Ensure forwarder is running with `forwarder.transport.mode=websocket`
- Check the WebSocket URL: `ws://localhost:8081/ws`
- Verify firewall allows WebSocket connections

### Client Not Receiving Messages (WebSocket)
- Check client logs for connection status
- Verify registration was successful
- Check forwarder logs for active sessions count

### REST Still Active in WebSocket Mode
- Ensure you've rebuilt after changing properties: `mvn clean package`
- Check logs for which mode is active

### ACK Timeout
- Increase timeout: `forwarder.websocket.ack.timeout.ms=20000`
- Check network latency
- Verify client is sending ACK messages

---

## Future Enhancements

- [ ] Add performance metrics comparison dashboard
- [ ] Support mixed mode (some clients REST, some WebSocket)
- [ ] Add compression for WebSocket messages
- [ ] Implement backpressure handling
- [ ] Add WebSocket authentication/authorization
- [ ] Support WebSocket over TLS (wss://)

---

## Summary

The WebSocket implementation is **complete and ready to use**. Both REST and WebSocket modes:
- Use the same database schema and delivery guarantees
- Support batching, retry, and ordered delivery
- Can be switched via simple configuration change
- Are production-ready for performance comparison

The key advantage of this architecture is that **all the reliability logic stays the same** - only the transport layer changes. This makes it easy to fairly compare the performance characteristics of REST vs WebSocket.

