# Kafka → Broker → External WebSocket Client demo (Java, Maven, Docker)

What you get: a self-contained test setup to simulate internal Kafka traffic being forwarded by a broker to an external client over WebSocket. The broker persists events to Postgres until the external client ACKs them, then broker deletes them.

## Components
- Kafka + Zookeeper (Docker)
- Postgres (Docker)
- Producer (Java) — produces JSON events into Kafka (`test-topic`)
- Broker (Java) — consumes Kafka, stores events in Postgres, forwards to external client via WebSocket, deletes on ACK
- External Client (Java) — WebSocket server; receives events and sends back ACK JSON `{"id":"...", "status":"ACK"}`

## Prereqs
- Docker & Docker Compose (Windows)
- Java 17
- Maven

## Quick start
1. Start Docker services:
   ```bash
   docker-compose up -d
   ```
   Wait a few seconds for services to become ready.

2. Build the project:
   ```bash
   mvn -T 1C -pl common,producer,broker,client -am package
   ```
   That creates 3 runnable jars under each module `target/*-jar-with-dependencies.jar`.

3. Run in this order:
   - External client:
     ```bash
     java -jar client/target/client-1.0-SNAPSHOT-jar-with-dependencies.jar
     ```
   - Broker:
     ```bash
     java -jar broker/target/broker-1.0-SNAPSHOT-jar-with-dependencies.jar
     ```
   - Producer:
     ```bash
     java -jar producer/target/producer-1.0-SNAPSHOT-jar-with-dependencies.jar
     ```

4. Inspect DB:
   ```bash
   docker exec -it $(docker ps -qf "ancestor=postgres:15") psql -U test -d brokerdb -c "select * from events;"
   ```

## Notes
- This is a **demo**. For production: add retries/backoff, durable websocket handling, TLS, idempotency checks, better schema, metrics, and proper docker network configuration.
