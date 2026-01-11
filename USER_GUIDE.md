# User guide

## 0. Prerequisites

Make sure following programs are installed:

- JDK 17 (or newer)
- Maven
- Docker

## 1. Set up infrastructure

```bash
docker-compose up -d
```

## 2. Build

```bash
mvn clean compile install -DskipTests
```

## 3. Start applications

```bash
mvn spring-boot:run -pl forwarder

mvn spring-boot:run -pl client

mvn spring-boot:run -pl producer
```

## Run with network conditions simulations

Start *Toxiproxy* containers:

```bash
docker compose --profile chaos up -d
```

Enable *Toxiproxy* in *clients* `application.properties` (uncomment the following line):

```properties
spring.profiles.active=toxiproxy
```

Set up the bad network params in `toxics.json` ([see](https://github.com/Shopify/toxiproxy)), e.g.:

```json
{
  "name": "latency_toxic",
  "type": "latency",
  "attributes": {
    "latency": 1000,
    "jitter": 200
  }
}
```

Then restart applications.

## Choosing modes

In `application.properties` of the *client* and *forwarder* set the following properties to one of the **rest**/**websocket**:

```bash
client.transport.mode=rest          # in client

forwarder.transport.mode=websocket  # in forwarder
```
