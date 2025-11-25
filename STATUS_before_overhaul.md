# Project Status Overview

**Project:** Kafka WebSocket Demo (HTTP Forwarder Version)  
**Last Updated:** November 17, 2025

## Project Architecture

This is a Kafka-based event distribution system with HTTP forwarding capabilities. The system consists of multiple Spring Boot microservices that work together to produce, consume, transform, and forward Kafka events.

## Modules Overview

### 1. **Common Module** ✅ COMPLETE
**Status:** Fully implemented  
**Purpose:** Shared data models and contracts

**Implemented:**
- `InternalData` record - Internal event format (msg, name)
- `ExternalData` record - External event format (msg, name, externalNew)
- `RegistrationRequest` record - Client registration payload (clientUrl, topics)

---

### 2. **Producer Module** ✅ COMPLETE
**Status:** Fully functional  
**Purpose:** Generates and publishes events to Kafka

**Implemented:**
- ✅ Spring Boot application with scheduling enabled
- ✅ Kafka producer configuration (JSON serialization)
- ✅ Kafka topic configuration
- ✅ `PublishService` - Scheduled event generation (every 3 seconds)
- ✅ Produces `InternalData` events to `topic1`
- ✅ Incremental counter for test data generation

**Configuration:**
- Port: 8080 (default)
- Kafka bootstrap: localhost:9092
- Topics: topic1, topic2

---

### 3. **Forwarder Module** ⚠️ PARTIALLY IMPLEMENTED
**Status:** Core infrastructure ready, business logic incomplete  
**Purpose:** Consumes Kafka events, transforms them, stores in DB, and forwards to registered clients

#### Implemented Components:

**✅ Database Layer:**
- PostgreSQL integration configured
- JPA entities: `Client` and `ExternalData`
- Repositories: `ClientRepository` and `ExternalDataRepository`
- `DbService` with basic save operations
- Client-ExternalData relationship (One-to-Many)
- Hibernate DDL auto-create enabled

**✅ Kafka Consumer:**
- Kafka consumer configuration (JSON deserialization)
- `KafkaService` listening to topics
- Consumer group: "main_group"

**✅ Registration Endpoint:**
- `/registration` POST endpoint
- Saves client information (IP, URL, subscribed topics)
- Uses `HttpServletRequest` to capture client IP

**⚠️ Missing/Incomplete:**

1. **KafkaService Logic** 🔴 NOT IMPLEMENTED
   - Currently only logs received events
   - No call to transformation/send services
   - TODO comments discuss two possible approaches for data persistence
   - No actual event processing implemented

2. **TransformationService** 🔴 EMPTY
   - Service exists but has no implementation
   - Should transform `InternalData` → `ExternalData`
   - No transformation logic written

3. **SendService** 🔴 EMPTY
   - Service exists with placeholder comment about WebClient
   - Should use Spring WebFlux WebClient for async HTTP calls
   - No actual sending logic implemented
   - Missing retry logic
   - Missing error handling

4. **Client Unregistration** 🔴 NOT IMPLEMENTED
   - TODO comment indicates need for unregister method
   - No DELETE endpoint for client removal
   - No REST-compliant client management

5. **Data Tracking** 🔴 NOT IMPLEMENTED
   - No logic to track which clients received which data
   - No mechanism to mark data as sent
   - No cleanup of successfully delivered data

6. **Topic-based Routing** 🔴 NOT IMPLEMENTED
   - Clients register with topic subscriptions
   - No logic to filter/route events based on client topic preferences

**Configuration:**
- Port: 8081
- Kafka bootstrap: localhost:9092
- Consumer topics: topic1
- PostgreSQL: localhost:5001/brokerdb (note: port workaround for WSL issue)

**Known Issues:**
- Port 5432 conflict with wslrelay.exe (workaround: using port 5001)

---

### 4. **Client Module** ⚠️ SKELETON ONLY
**Status:** Minimal structure, no functionality  
**Purpose:** Simulates external clients that receive forwarded events

**Implemented:**
- ✅ Spring Boot application skeleton
- ✅ `DataController` with `/data` POST endpoint (empty)
- ✅ `SimulationService` (empty)
- ✅ `SendService` (empty, placeholder for WebClient)

**🔴 Missing:**
- No data reception logic
- No registration logic to register with forwarder
- No data processing/logging
- No simulation of client behavior
- No WebClient implementation for registration

---

### 5. **Broker Module** ❓ UNCLEAR STATUS
**Status:** Appears to be legacy/unused  
**Notes:**
- Has compiled artifacts in target/ directory
- No source code found in src/ directory
- Not listed in root pom.xml modules
- Possibly outdated or removed module
- **Recommendation:** Remove if not needed, or clarify its purpose

---

## Infrastructure

### Docker Compose ✅ CONFIGURED
**Services:**
- **Kafka** (Apache Kafka latest)
  - Port: 9092
  - Container: broker
- **PostgreSQL** (v17)
  - Port: 5001 → 5432 (mapped due to WSL conflict)
  - Container: postgres
  - Database: brokerdb
  - Health check configured

**Configuration:** Uses `.env` file for environment variables

---

## Overall System Flow

### Intended Flow:
1. **Producer** generates events → Kafka topic1
2. **Forwarder** consumes events from Kafka
3. Events transformed (InternalData → ExternalData)
4. Events stored in PostgreSQL
5. Events forwarded to registered clients via HTTP POST
6. Delivered events tracked and cleaned up

### Currently Working:
1. ✅ Producer generates and sends events to Kafka
2. ✅ Forwarder consumes events (logs them only)
3. ✅ Forwarder can register clients
4. ✅ Database schema created automatically

### Not Working:
1. 🔴 Event transformation
2. 🔴 Event forwarding to clients
3. 🔴 Delivery tracking
4. 🔴 Client registration from client module
5. 🔴 Client data reception

---

## Priority Tasks (Recommended Order)

### High Priority:
1. **Implement TransformationService**
   - Add method to convert InternalData → ExternalData
   - Add "externalNew" field population logic

2. **Implement SendService**
   - Configure WebClient bean
   - Implement async HTTP POST to client URLs
   - Add retry logic and error handling
   - Add logging for sent/failed deliveries

3. **Complete KafkaService Logic**
   - Call transformation service
   - Save to database
   - Call send service
   - Implement chosen persistence strategy (see TODO comments)

4. **Implement Topic-based Routing**
   - Filter events based on client subscribed topics
   - Only forward events clients are interested in

5. **Implement Delivery Tracking**
   - Track which clients received which events
   - Clean up successfully delivered events
   - Handle failed deliveries

### Medium Priority:
6. **Complete Client Module**
   - Implement data reception in DataController
   - Add registration logic to register with forwarder
   - Add logging/simulation of received data

7. **Add Client Unregistration**
   - Implement DELETE /registration endpoint
   - Clean up client data on unregister

8. **Add Error Recovery**
   - Handle forwarder crashes (redelivery)
   - Handle client unavailability
   - Dead letter queue for failed events

### Low Priority:
9. **Clean up Broker Module**
   - Remove if unused
   - Document if needed

10. **Add Monitoring/Health Checks**
    - Metrics for event processing
    - Client connectivity status
    - Database health

---

## Technical Stack

- **Language:** Java 17
- **Framework:** Spring Boot 3.5.7
- **Build Tool:** Maven
- **Message Broker:** Apache Kafka (latest)
- **Database:** PostgreSQL 17
- **Async HTTP:** Spring WebFlux (WebClient)
- **ORM:** Spring Data JPA with Hibernate

---

## Getting Started

### Prerequisites:
- Java 17+
- Maven
- Docker & Docker Compose

### Run Instructions:
1. Start infrastructure: `docker-compose up -d`
2. Build all modules: `mvn clean install`
3. Run producer: `mvn spring-boot:run -pl producer`
4. Run forwarder: `mvn spring-boot:run -pl forwarder`
5. Run client(s): `mvn spring-boot:run -pl client`

### Testing:
- Use kafkacat to monitor topics: `kafkacat -b localhost:9092 -t topic1`
- Check logs for event production/consumption

---

## Notes & TODOs from Code

- **KafkaService:** Two design approaches discussed in comments (Option 1 vs Option 2 for persistence)
- **KafkaConsumerConfig:** Question about whether manual config is needed vs Spring Boot auto-config
- **JsonDeserializer:** Using wildcard trusted packages (`*`) - security consideration for production
- **Database:** Using `create-drop` - NOT suitable for production
- **Port Conflict:** WSL wslrelay.exe stealing port 5432

---

## Summary

**Overall Completion:** ~35%

- ✅ Infrastructure & Configuration: 90%
- ✅ Producer: 100%
- ⚠️ Forwarder: 40% (structure done, logic missing)
- ⚠️ Client: 10% (skeleton only)
- ❓ Broker: Unknown/Unused

**Main Blockers:**
The core business logic for event transformation, forwarding, and delivery tracking is not implemented. The system can produce and consume events but cannot forward them to clients yet.

