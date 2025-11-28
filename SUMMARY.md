# Kafka WebSocket Demo - System Summary

**Project:** Event Distribution System with Kafka, PostgreSQL, and WebSocket Forwarding  
**Last Updated:** November 28, 2025

---

## 🏗️ System Architecture Overview

This is a **reliable event distribution system** that receives events from Kafka and forwards them to multiple WebSocket clients with **guaranteed delivery**, **ordering**, and **automatic retry**.

```
┌──────────┐      ┌───────┐      ┌───────────┐      ┌────────────┐
│ Producer │ ───> │ Kafka │ ───> │ Forwarder │ <───>│ Client(s)  │
└──────────┘      └───────┘      └───────────┘      └────────────┘
                                       ▲
                                       │                    
                                       ▼                    
                                  ┌──────────┐             
                                  │PostgreSQL│  
                                  └──────────┘        
```

### Key Features:
- ✅ **At-least-once delivery** guarantee to clients
- ✅ **Ordered delivery** using Kafka offsets as sequence numbers
- ✅ **Automatic retry** with configurable attempts and intervals
- ✅ **WebSocket STOMP confirmation** (via /app/confirm)
- ✅ **Client registration** with topic subscription via WebSocket
- ✅ **Database persistence** for crash recovery
- ✅ **Idempotent operations** (client re-registration)
- ✅ **Disconnect handling** - no attempts to disconnected clients

---

## 📊 Data Models

### **1. ExternalDataTableEntry** (Events)
The transformed event data ready to be sent to clients.

```java
@Entity
@Table(indexes = {
    @Index(name = "idx_external_data_sequence", columnList = "sequenceNumber"),
    @Index(name = "idx_external_data_topic", columnList = "topic")
})
public class ExternalDataTableEntry {
    private Long id;                    // Primary key
    private String topic;               // Kafka topic name
    private String msg;                 // Message content
    private String name;                // Event name/identifier
    private String externalNew;         // Transformed field
    private Long sequenceNumber;        // Kafka offset (for ordering)
    private LocalDateTime receivedAt;   // When received from Kafka
}
```

**Purpose:** Stores event data that needs to be delivered to clients. Persisted immediately upon receiving from Kafka to prevent data loss.

**Lifecycle:**
1. Created when event arrives from Kafka
2. Kept until ALL clients confirm receipt
3. Deleted when all associated DeliveryStatus records are confirmed

---

### **2. DeliveryStatus** (Delivery Tracking)
Tracks the delivery status of each event to each client.

```java
@Entity
@Table(
    uniqueConstraints = {
        @UniqueConstraint(columnNames = {"external_data_id", "client_id"})
    },
    indexes = {
        @Index(name = "idx_delivery_last_attempt", columnList = "lastAttempt, confirmed"),
        @Index(name = "idx_delivery_external_data", columnList = "external_data_id")
    }
)
public class DeliveryStatus {
    private Long id;                    // Primary key
    private Long externalDataId;        // FK to ExternalDataTableEntry
    private Long clientId;              // FK to Client
    private boolean confirmed;          // Has client confirmed receipt?
    private LocalDateTime lastAttempt;  // Last delivery attempt time
    private int attemptCount;           // Number of delivery attempts
}
```

**Purpose:** One record per (event, client) pair. Tracks whether each client has received and confirmed each event.

**Lifecycle:**
1. Created when event is first sent to a client
2. Updated on each retry attempt
3. Marked confirmed when client returns HTTP 200
4. Deleted when parent ExternalDataTableEntry is deleted

**Unique Constraint:** `(external_data_id, client_id)` - prevents duplicate delivery tracking

---

### **3. Client** (Registered Clients)
Clients that receive forwarded events.

```java
@Entity
@Table(
    uniqueConstraints = {
        @UniqueConstraint(columnNames = "client_identifier")
    },
    indexes = {
        @Index(name = "idx_client_identifier", columnList = "client_identifier")
    }
)
public class Client {
    private Long id;                        // Primary key
    private String clientIdentifier;        // Unique client identifier
    private String sessionId;               // WebSocket session ID (null if disconnected)
    private List<String> subscribedTopics;  // Kafka topics subscribed to
}
```

**Purpose:** Stores registered clients and their topic subscriptions.

**Lifecycle:**
1. Created on first registration
2. Updated (upsert) on subsequent registrations from same client
3. sessionId set on connect, cleared on disconnect
4. Persists across forwarder restarts

**Unique Constraint:** `client_identifier` - prevents duplicate client registrations

---

## 🔄 Complete Event Flow

### **Phase 1: Event Production**

```
Producer Service (Port 8080)
  │
  ├─ Scheduled every 2000ms (configurable: producer.rate.period.ms)
  ├─ Creates InternalData(msg="test", name="nameX")
  └─ Sends to Kafka topic1
```

**Code:** `producer/PublishService.java`

---

### **Phase 2: Kafka Consumption & Transformation**

```
KafkaService (Forwarder)
  │
  ├─ @KafkaListener receives ConsumerRecord from Kafka
  ├─ Extracts: message, topic, offset (sequence number)
  │
  ├─ [STEP 1] Transform InternalData → ExternalDataTableEntry
  │   └─ TransformationService.transform(data, topic, offset)
  │       └─ Adds "externalNewValue" field
  │
  ├─ [STEP 2] Save to database IMMEDIATELY
  │   └─ dbService.saveExternalData(externalData)
  │       └─ Protection against data loss if forwarder crashes
  │
  ├─ [STEP 3] Find subscribed clients
  │   └─ dbService.getClientsByTopic(topic)
  │       └─ Filters clients where subscribedTopics.contains(topic)
  │
  └─ [STEP 4] For each client:
      ├─ Create DeliveryStatus(externalDataId, clientId)
      └─ sendToClientAsync(data, client, status)
```

**Code:** `forwarder/kafka/KafkaService.java`

**Database State After This Phase:**
```sql
ExternalDataTableEntry: 1 row (the event)
DeliveryStatus: N rows (one per subscribed client)
```

---

### **Phase 3: Sending to Clients**

```
SendService
  │
  ├─ Creates ExternalData payload (common model)
  ├─ WebSocket STOMP send to /queue/data/{clientId}
  │   └─ Body: { "id": "...", "msg": "...", "name": "...", "externalNew": "..." }
  │
  ├─ SUCCESS (client receives and confirms):
  │   └─ Client sends STOMP message to /app/confirm
  │       └─ ConfirmationController.handleConfirmation()
  │           └─ dbService.markAsConfirmedByClientId(dataId, clientId)
  │               ├─ Sets DeliveryStatus.confirmed = true
  │               └─ If ALL clients confirmed:
  │                   └─ DELETE ExternalDataTableEntry + all DeliveryStatus
  │
  └─ FAILURE (client disconnected):
      └─ No send attempt (checked sessionId != null)
```

**Code:** `forwarder/SendService.java`, `forwarder/ConfirmationController.java`

**Key Point:** STOMP confirmation message = confirmation signal!

---

### **Phase 4: Client Reception**

```
Client WebSocketService (Port 8082)
  │
  ├─ Connects to WebSocket at forwarder:8080/ws
  ├─ Subscribes to /user/queue/data (but mapped to /queue/data/{clientId})
  ├─ Sends registration STOMP message to /app/register
  │   └─ Body: { "clientIdentifier": "...", "topics": ["topic1"] }
  │
  ├─ Receives ExternalData via STOMP
  ├─ Logs the received data
  ├─ Processes/stores the data (application-specific)
  └─ Sends confirmation STOMP message to /app/confirm
      └─ Body: { "id": "...", "clientIdentifier": "..." }
```

**Code:** `client/WebSocketService.java`

---

### **Phase 5: Retry Mechanism**

```
ResendService (Scheduled)
  │
  ├─ Runs every 5000ms (configurable: forwarder.retry.check.interval.ms)
  │
  ├─ Query database for pending deliveries:
  │   └─ WHERE confirmed = false
  │   └─ AND lastAttempt < (now - 5000ms)
  │   └─ AND attemptCount < 5
  │   └─ ORDER BY sequenceNumber ASC  ← Maintains order!
  │
  ├─ For each pending delivery:
  │   ├─ Check if client is connected (sessionId != null)
  │   │   └─ Skip if disconnected
  │   ├─ Update lastAttempt = now
  │   ├─ Increment attemptCount
  │   ├─ Send to client (same as Phase 3)
  │   └─ If attemptCount >= maxAttempts:
  │       └─ Log error and give up
  │
  └─ Cleanup: Delete events that failed max attempts
```

**Code:** `forwarder/ResendService.java`

**Configuration:**
- `forwarder.retry.check.interval.ms=5000` - How often to check for retries
- `forwarder.retry.interval.ms=5000` - Wait time before retrying
- `forwarder.retry.max.attempts=5` - Maximum retry attempts

---

## 🗄️ Database Schema

### Tables & Relationships

```
┌─────────────────────────────┐
│ ExternalDataTableEntry      │
├─────────────────────────────┤
│ id (PK)                     │
│ topic                       │
│ msg                         │
│ name                        │
│ externalNew                 │
│ sequenceNumber (INDEXED)    │────┐
│ receivedAt                  │    │
└─────────────────────────────┘    │
                                   │
                                   │ N
┌─────────────────────────────┐    │
│ DeliveryStatus              │    │
├─────────────────────────────┤    │
│ id (PK)(is it really needed?│    │
│ externalDataId (FK) INDEXED │◄───┘
│ clientId (FK) INDEXED       │◄───┐
│ confirmed                   │    │
│ lastAttempt (INDEXED)       │    │
│ attemptCount                │    │
│ UNIQUE(externalDataId,      │    │
│        clientId)            │    │
└─────────────────────────────┘    │
                                   │ N
┌─────────────────────────────┐    │
│ Client                      │    │
├─────────────────────────────┤    │
│ id (PK)                     │────┘
│ clientIdentifier UNIQUE     │
│ sessionId                   │
│ subscribedTopics (JSON)     │
└─────────────────────────────┘
```

### Indices (For Performance)* 

| Table | Index | Purpose |
|-------|-------|---------|
| DeliveryStatus | `(lastAttempt, confirmed)` | Fast retry queries |
| DeliveryStatus | `(externalDataId)` | Fast confirmation counting |
| DeliveryStatus | `(externalDataId, clientId)` | Fast confirmation lookups |
| ExternalDataTableEntry | `(sequenceNumber)` | Ordered event retrieval |
| ExternalDataTableEntry | `(topic)` | Topic-based queries |
| Client | `(clientIdentifier)` | Fast client lookups |

---

## 🎯 Key Components

### **Producer Module**
- **PublishService** - Generates test events on schedule
- Sends `InternalData` to Kafka

### **Forwarder Module** (The Core)
- **KafkaService** - Consumes from Kafka, orchestrates flow
- **TransformationService** - Transforms InternalData → ExternalDataTableEntry
- **SendService** - WebSocket STOMP messaging to send data to clients
- **ResendService** - Scheduled retry of failed deliveries
- **RegistrationController** - Accepts client registrations via STOMP
- **ConfirmationController** - Handles client confirmations via STOMP
- **DisconnectEventListener** - Handles WebSocket disconnect events
- **WebSocketConfig** - WebSocket broker configuration
- **DbService** - Database operations (CRUD + business logic)

### **Client Module**
- **WebSocketService** - Manages WebSocket connection, registration, and data reception

---

## 🔒 Reliability Mechanisms

### **1. Database Persistence**
- Events saved to DB **immediately** upon Kafka consumption
- Survives forwarder crashes/restarts
- DeliveryStatus tracks per-client delivery state

### **2. Automatic Retry**
- Failed deliveries automatically retried every 5 seconds
- Up to 5 attempts per delivery
- Exponential backoff can be added if needed

### **3. Ordered Delivery**
- Kafka offset stored as `sequenceNumber`
- Retry queries ordered by `sequenceNumber ASC`
- Guarantees FIFO delivery per topic, at least once

### **4. Idempotent Operations**
- Client re-registration updates existing record (upsert)
- Duplicate confirmation ignored (not necessary for now, we are using STOMP confirmation)
- Safe to restart any component

### **5. Client Registration Retry**
- Client retries registration up to 5 times if forwarder is down
- Linear backoff: 2s, 4s, 6s, 8s, 10s
- Handles forwarder temporary unavailability during startup

### **6. Disconnect Handling**
- WebSocket disconnect events clear `sessionId` in Client table
- Send and resend operations skip clients with `sessionId = null`
- Prevents wasted attempts on disconnected clients
- Clients reconnect and re-register automatically

---

## ⚙️ Configuration

### **Forwarder** (`forwarder/application.properties`)

```properties
# Server
server.port=8081

# Kafka
spring.kafka.bootstrap-servers=localhost:9092
kafka.topics=topic1
spring.kafka.consumer.group-id=main_group

# PostgreSQL
spring.datasource.url=jdbc:postgresql://localhost:5001/brokerdb
spring.datasource.username=test
spring.datasource.password=test
spring.jpa.hibernate.ddl-auto=update  # Persists data across restarts

# Retry Configuration (all in milliseconds)
forwarder.retry.check.interval.ms=5000      # Retry check frequency
forwarder.retry.interval.ms=5000            # Wait before retry
forwarder.retry.max.attempts=5              # Max attempts per delivery

# WebSocket
# No additional config needed - uses Spring defaults
```

### **Producer** (`producer/application.properties`)

```properties
server.port=8080
spring.kafka.bootstrap-servers=localhost:9092
producer.rate.period.ms=2000                # Event generation rate
```

### **Client** (`client/application.properties`)

```properties
server.port=8082
forwarder.url=ws://localhost:8081/ws        # WebSocket URL
client.subscribed.topics=topic1             # Comma-separated topics
client.auto.register=true                   # Auto-register on startup

# Registration Retry
client.registration.retry.max.attempts=5
client.registration.retry.delay.ms=2000
```

---

## 🌐 WebSocket STOMP Endpoints

### **Forwarder (Port 8081)**

#### WebSocket Endpoint: `/ws`
Main WebSocket connection endpoint with SockJS fallback.

#### STOMP Destinations:

**Client Registration:**
- **Destination:** `/app/register`
- **Method:** STOMP SEND
- **Request:**
```json
{
  "clientIdentifier": "client1",
  "topics": ["topic1", "topic2"]
}
```

**Client Confirmation:**
- **Destination:** `/app/confirm`
- **Method:** STOMP SEND
- **Request:**
```json
{
  "id": 123,
  "clientIdentifier": "client1"
}
```

**Data Reception (by clients):**
- **Subscription:** `/user/queue/data`
- **Method:** STOMP SUBSCRIBE
- **Response:**
```json
{
  "id": 123,
  "msg": "test",
  "name": "name123",
  "externalNew": "externalNewValue"
}
```

**Behavior:** 
- Registration upserts client (updates if already exists)
- Confirmation marks delivery as successful
- Data sent to `/queue/data/{clientId}` internally

---

### **Client (Port 8082)**

The client is a WebSocket STOMP client that:
- Connects to forwarder's `/ws` endpoint
- Subscribes to `/user/queue/data` for receiving data
- Sends registration to `/app/register`
- Sends confirmations to `/app/confirm`

---

## 📈 Performance Considerations

### **Database Indices**
All critical query paths should be indexed, change `model/` classes accordingly:
- Retry queries: `(lastAttempt, confirmed)`
- Confirmation counting: `(externalDataId)`
- Client lookups: `(clientIdentifier)`
- Ordering: `(sequenceNumber)`

### **WebSocket Messaging**
- STOMP over WebSocket for real-time bidirectional communication
- Automatic SockJS fallback for browser compatibility
- Persistent connections reduce overhead compared to HTTP polling

### **Async STOMP Sends**
- SimpMessagingTemplate for non-blocking message sending
- No blocking on client confirmation (fire-and-forget with retry)
- Doesn't block Kafka consumption

### **Batch Cleanup**
- Failed deliveries should be cleaned in batch by ResendService
- Confirmed events deleted immediately when all clients confirm

### **Connection Pooling, maybe should be used, as multiple connections to DB might be desired**
```properties
spring.datasource.hikari.maximumPoolSize=5
spring.datasource.hikari.connectionTimeout=20000
```

---

## 🐛 Edge Cases Handled

### **1. Client Restart**
- Client re-registers (upsert prevents duplicates)
- Continues receiving events normally

### **2. Forwarder Restart**
- Undelivered events persist in database
- ResendService picks them up automatically
- Events delivered in order via `sequenceNumber`

### **3. No Clients Subscribed**
- Event saved to DB
- Immediately deleted (no subscribers)
- Prevents database bloat

### **4. Client Temporarily Down**
- Events queued in database
- Automatic retry every 5 seconds
- Up to 5 attempts before giving up

### **5. Forwarder Down During Client Startup**
- Client retries WebSocket connection automatically
- Spring WebSocket client handles reconnection
- Logs error if connection fails

### **6. WebSocket Disconnect**
- DisconnectEventListener clears sessionId
- Send operations skip disconnected clients
- Client can reconnect and re-register

### **7. Duplicate Client Registration**
- Unique constraint on `clientIdentifier`
- Upsert logic updates existing record

---

## 🚀 Running the System

### **1. Start Infrastructure**
```bash
docker-compose up -d
```
Starts: Kafka (port 9092), PostgreSQL (port 5001)
```bash
mvn clean compile
```
Builds all modules
### **2. Start Services**

**Terminal 1 - Producer:**
```bash
mvn spring-boot:run -pl producer
```

**Terminal 2 - Forwarder:**
```bash
mvn spring-boot:run -pl forwarder
```

**Terminal 3 - Client:**
```bash
mvn spring-boot:run -pl client
```

### **3. Observe Logs**
- **Producer:** Sends event every 2 seconds to Kafka
- **Forwarder:** Receives → Transforms → Sends via WebSocket → Receives confirmations
- **Client:** Connects via WebSocket, receives and confirms data

---

## 📊 Monitoring

### **Key Metrics to Watch:**
- Number of pending deliveries: `SELECT COUNT(*) FROM delivery_status WHERE confirmed = false`
- Failed deliveries: `SELECT COUNT(*) FROM delivery_status WHERE attempt_count >= 5`
- Event queue depth: `SELECT COUNT(*) FROM external_data_table_entry`
- Registered clients: `SELECT COUNT(*) FROM client`

### **Health Indicators:**
- ✅ Pending deliveries should be near zero under normal operation
- ✅ Event queue should not grow indefinitely
- ⚠️ High attempt counts indicate client connectivity issues
- ❌ Growing queue indicates forwarder can't keep up

---

## 🔮 Future Enhancements

### **Not Yet Implemented:**
1. **Circuit Breaker** - Stop trying clients that are consistently down
2. **Dead Letter Queue** - Separate storage for permanently failed events
3. **Metrics/Observability** - Prometheus metrics, health endpoints
4. **Orphaned Data Cleanup** - Scheduled job to clean data with no DeliveryStatus
5. **Request ID Tracking** - Correlation IDs for end-to-end tracing
6. **Multi-partition Support** - Currently assumes single partition ordering
7. **Client Unregister Endpoint** - DELETE /registration/{clientId}

---

## 🎓 Design Decisions

### **Why STOMP Confirmation?**
- **Integrated:** Uses same WebSocket connection
- **Real-time:** Immediate confirmation via messaging

### **Why Store Kafka Offset as sequenceNumber?**
- **Ordering:** Guarantees FIFO delivery even after crashes
- **Natural:** Kafka already guarantees offset ordering per partition
- **Efficient:** Single Long field, indexed for fast sorting

### **Why DeliveryStatus Per Client?**
- **Granularity:** Track delivery to each client independently
- **Partial Failure:** One client down doesn't block others
- **Cleanup:** Delete event only when ALL clients confirm

### **Why Immediate Database Save?**
- **Durability:** Survives forwarder crashes
- **At-least-once:** Guarantees event isn't lost
- **Trade-off:** Slight performance cost for reliability

---

## 📝 Summary

This system provides **reliable, ordered event distribution** from Kafka to multiple WebSocket clients with:
- ✅ Database-backed persistence
- ✅ Automatic retry with configurable limits
- ✅ Order preservation via Kafka offsets
- ✅ STOMP confirmation over WebSocket
- ✅ Client registration and topic subscription
- ✅ Crash recovery and idempotent operations
- ✅ Disconnect handling for efficiency
---

## Actual TODOs (not generated by LLM)
- [ ] Remove PK from DeliveryStatus since we use composite (externalDataId, clientId)
- [ ] Batch client resend instead of one-by-one
- [x] Try the Websocket approach
- [ ] Implement InternalData -> ExternalData transformation logic
- [ ] InternalData generator (1kB)
- [ ] Load testing (for now change `producer.rate.period.ms=2000`)

---

And by the way, forwarder.ExternalData was renamed to **forwarder.ExternalDataTableEntry** to avoid confusion with common.ExternalData


