# WebSocket Architecture - Different from REST

## The Problem You Identified

You were absolutely right to question the architecture! The original WebSocket implementation tried to use the same concurrency model as REST, which created fundamental issues:

### Original Architecture (WRONG):
```
10 Kafka Threads → 10 DB Threads → All trying to send through WebSocket sessions
                                   ↓
                            Locks, queues, CompletableFutures
                                   ↓
                            Single WebSocket session (1 thread only)
```

**Issues:**
- WebSocket sessions can only handle **one message at a time** (thread-safety requirement)
- Multiple threads were competing with locks to send through the same session
- Artificial concurrency limits (`maxConcurrentRequests`) that don't make sense for WebSockets
- Complex ACK management shared across all sessions
- Unnecessary CompletableFuture overhead

## New Architecture (CORRECT)

### REST Mode Architecture:
```
10 Kafka Threads → 10 DB Threads → HTTP Connection Pool (500 concurrent)
                                   ↓
                            Many simultaneous HTTP requests
                            (naturally thread-safe)
```

**Why it works:**
- HTTP is **request-response** with connection pooling
- Each request is independent
- Connection pool handles concurrency naturally
- Makes sense to have `maxConcurrentRequests=500`

### WebSocket Mode Architecture:
```
10 Kafka Threads → 10 DB Threads → Queue messages for clients
                                   ↓
                    Per-Client Dedicated Sender Threads
                                   ↓
        Client 1 Thread + Queue → WebSocket Session 1
        Client 2 Thread + Queue → WebSocket Session 2
        Client 3 Thread + Queue → WebSocket Session 3
        ...
```

**Why it works:**
- Each WebSocket session has its **own dedicated sender thread**
- Each sender has its **own queue** (BlockingQueue, 10,000 capacity)
- Multiple Kafka/DB threads can safely **queue** messages (lock-free)
- Each sender thread **sequentially sends** from its queue (no locks needed)
- Each sender manages its **own ACKs** independently
- Natural backpressure: if queue is full, message fails and retry handles it

## Key Architectural Differences

| Aspect | REST Mode | WebSocket Mode |
|--------|-----------|----------------|
| **Concurrency Model** | Connection pool (500 concurrent) | Per-session threads (N clients) |
| **Send Operation** | Blocking HTTP request | Queue message (non-blocking) |
| **Capacity Limit** | Global `maxConcurrentRequests` | Per-session queue depth |
| **Thread Safety** | Connection pool handles it | Each session = 1 dedicated thread |
| **Backpressure** | Reject when pool full | Reject when session queue full |
| **ACK Management** | Global shared map | Per-sender isolated |
| **Complexity** | Medium (HTTP client handles much) | Low (simple queue→send loop) |

## Implementation Details

### 1. WebSocketSessionSender (NEW)
**File:** `WebSocketSessionSender.java`

Each client gets one instance with:
- **Dedicated thread** running a send loop
- **BlockingQueue<SendTask>** (10,000 capacity)
- **Own ACK tracking** (ConcurrentHashMap)
- **Own timeout executor** (ScheduledExecutorService)

```java
// Multiple Kafka threads can safely queue messages
CompletableFuture<Boolean> future = sender.queueBatch(dataList);

// Sender thread loop (runs independently)
while (running) {
    SendTask task = sendQueue.poll(1, SECONDS);
    if (task != null) {
        session.sendMessage(new TextMessage(json));  // Only this thread sends!
        registerAckTimeout(task);
    }
}
```

**Benefits:**
- No locks on sending (only this thread sends)
- Lock-free queuing (BlockingQueue is thread-safe)
- Simple, predictable behavior
- Easy to debug (one thread per client)

### 2. WebSocketSessionManager (REFACTORED)
**File:** `WebSocketSessionManager.java`

Changed from storing sessions to managing senders:

```java
// Old: Map<Long, WebSocketSession>
// New: Map<Long, WebSocketSessionSender>

public void addSession(Long clientId, String identifier, WebSocketSession session) {
    WebSocketSessionSender sender = new WebSocketSessionSender(...);
    sender.start();  // Starts dedicated thread
    senders.put(clientId, sender);
}

public void removeSession(Long clientId) {
    sender.shutdown();  // Gracefully stops thread
}
```

**Benefits:**
- Clean lifecycle management
- Automatic cleanup on disconnect
- Each sender is independent

### 3. WebSocketSendService (SIMPLIFIED)
**File:** `WebSocketSendService.java`

Massively simplified - just routes to the correct sender:

```java
public CompletableFuture<BatchSendResult> sendBatchToClient(...) {
    WebSocketSessionSender sender = sessionManager.getSender(clientId);
    if (sender == null) {
        return CompletableFuture.completedFuture(failure);
    }
    
    // Just queue it - sender handles the rest
    return sender.queueBatch(dataList)
        .thenApply(success -> new BatchSendResult(success, ...));
}
```

**Removed:**
- ❌ Global `maxConcurrentRequests` limit
- ❌ Global `activeRequests` counter
- ❌ Shared `pendingAcks` map
- ❌ Session locks
- ❌ Complex ACK coordination

**Benefits:**
- ~70% less code
- No artificial concurrency limits
- No global state
- Each sender is isolated

## Performance Characteristics

### REST Mode:
- **Throughput**: Limited by connection pool size (500 concurrent)
- **Latency**: HTTP overhead + network RTT
- **Scalability**: Scales with connection pool
- **Best for**: Many small clients, unreliable networks

### WebSocket Mode:
- **Throughput**: Limited by per-session queue processing
- **Latency**: Minimal (persistent connection, no HTTP overhead)
- **Scalability**: Scales with number of clients (1 thread each)
- **Best for**: Few high-throughput clients, real-time needs

### Thread Usage Comparison:

**REST Mode:**
- 10 Kafka consumer threads
- 10 DB worker threads
- ~20-30 HTTP client threads (from connection pool)
- **Total: ~40-50 threads**

**WebSocket Mode:**
- 10 Kafka consumer threads
- 10 DB worker threads
- N sender threads (1 per connected client)
- N timeout executors (1 per connected client)
- **Total: 20 + (2 × N clients)**

If you have 100 clients:
- REST: ~50 threads
- WebSocket: ~220 threads (but much simpler per thread)

## Configuration

### REST Mode (`application.properties`):
```properties
forwarder.transport.mode=rest
forwarder.client.timeout.ms=10000
forwarder.client.max.concurrent.requests=500  # Global limit
forwarder.client.max.batch.size=100
```

### WebSocket Mode (`application.properties`):
```properties
forwarder.transport.mode=websocket
forwarder.websocket.ack.timeout.ms=10000
forwarder.websocket.endpoint=/ws
forwarder.client.max.batch.size=100  # Still used for initial batching
```

**Note:** `max.concurrent.requests` is **ignored** in WebSocket mode - capacity is per-session.

## When to Use Which?

### Use REST Mode When:
- ✅ You have **many clients** (hundreds or thousands)
- ✅ Clients are **behind NATs/firewalls** (WebSocket may be blocked)
- ✅ Clients connect **intermittently** (HTTP is stateless)
- ✅ You need **horizontal scaling** (load balancer friendly)
- ✅ Network is **unreliable** (HTTP retry is simpler)

### Use WebSocket Mode When:
- ✅ You have **few clients** (dozens, maybe low hundreds)
- ✅ You need **low latency** (persistent connection)
- ✅ You need **high throughput per client** (no HTTP overhead)
- ✅ Clients stay **connected long-term** (stable network)
- ✅ You want **bidirectional communication** (client can push back)

## Migration Notes

If switching between modes:

1. **Change configuration:**
   ```properties
   forwarder.transport.mode=websocket  # or rest
   ```

2. **Restart forwarder:**
   - Spring will activate the correct `@ConditionalOnProperty` beans
   - Only one `MessageSender` implementation will be active

3. **Update clients:**
   - REST clients use HTTP POST endpoints
   - WebSocket clients use WebSocket protocol
   - Can't mix modes (all clients must use same transport)

## Summary

The key insight is that **WebSocket is fundamentally different from HTTP**:

- **HTTP**: Many concurrent requests, connection pool, request-response
- **WebSocket**: Persistent connection, **single-threaded sending**, bidirectional

The new architecture respects WebSocket's nature:
- ✅ One thread per session (matches WebSocket's constraints)
- ✅ Simple queue-based communication (no complex locking)
- ✅ Independent ACK management (no shared state)
- ✅ Natural backpressure (queue depth per client)
- ✅ Much simpler code (easier to maintain and debug)

**Result:** Simpler, faster, more correct WebSocket implementation that properly reflects the architectural differences between REST and WebSocket communication patterns.

