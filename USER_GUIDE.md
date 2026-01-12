# User guide
Quick start guide for running the event producer, forwarder, and client applications with Kafka and optional network fault simulations.
Via `app.properties` you can 
    - configure toxiproxy settings to simulate network faults,
    - pick between H2 and PostgreSQL databases for the forwarder,
    - choose between REST and WebSocket transport modes between client and forwarder,
    - configure load patterns (constant rate or ramp-up) for the producer,


## 0. Prerequisites
Make sure following programs are installed:
- JDK 17 (or newer)
- Maven
- Docker

This repo contains three runnable services (`producer`, `forwarder`, `client`) plus shared code in `common/` — users usually run them to produce events, forward them, and receive them respectively.
## 1. Set up infrastructure
```bash
docker-compose up -d
```
Running `docker-compose` starts Kafka and optional databases, and (with the `chaos` profile) starts Toxiproxy to simulate network faults.

## 2. Build
```bash
mvn clean compile install -DskipTests
```
Build produces the runnable JARs for `producer`, `forwarder`, and `client` under their respective `target/` directories.

## 3. Start applications
```bash
mvn spring-boot:run -pl forwarder

mvn spring-boot:run -pl client

mvn spring-boot:run -pl producer
```

Start `forwarder` first so it can accept client registrations; then start `client` and `producer` to register and generate events.
You can also run the JARs directly from `*/target/*.jar` if you prefer `java -jar`.


## Run with network conditions simulations
Toxiproxy is a lightweight proxy that can inject 'toxics' (latency, bandwidth limits, disconnects) between services so you can test resilience. A 'toxic' is a JSON definition telling Toxiproxy which proxy to affect and which fault to apply.
Set up the bad network params in `toxics.json` ([see](https://github.com/Shopify/toxiproxy)), e.g.:
Example latency toxic (adds 1000ms latency + 200ms jitter):
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

After editing the `toxics.json`, (re)start *Toxiproxy* containers:
```bash
docker compose --profile chaos up -d
```
Too check that *Toxiproxy* is running with the defined toxics use:
```bash
curl http://localhost:8474/proxies
```
Enable *Toxiproxy* in  `client/application.properties` (uncomment the following line):
```properties
spring.profiles.active=toxiproxy
```
Then rebuild and restart the applications.


## Choosing modes
You can choose between two transport modes for communication between *client* and *forwarder*: **REST** and **WebSocket**.
In `client/application.properties` and `forwarder/application.properties` set the following properties to `rest` or `websocket` (both must match):
```bash
client.transport.mode=rest/websocket          # in client
forwarder.transport.mode=rest/websocket       # in forwarder
```

## Testing load

The producer can emit events either at a constant rate or following a ramp-up (gradually increasing rate). 
The ramp-up behaviour is configured with properties in `producer/application.properties`:

- `rampup.enabled` (true/false) — enable ramp-up mode; false means the producer uses a constant rate = start rate.
- `rampup.start.rate` — starting events per second.
- `rampup.end.rate` — target events per second at the end of ramp-up.
- `rampup.duration.seconds` — how long (in seconds) the ramp takes.
- `rampup.stop.after` — whether the producer stops when the ramp completes, or continues at the end rate forever.

- **NOTE!** To gather reference metrics, you also need to update these same properties in `forwarder/rampup.properties` so the forwarder can produce CSV metrics file with expected rates as a reference.

Implementation notes: the producer uses a Poisson process to space events (randomized inter-arrival intervals) which approximates natural traffic bursts.

Quick run examples:
```bash
# Run a ramp-up load from the default properties
mvn spring-boot:run -pl producer

# Override properties on the fly (example: shorter ramp and higher target rate)
mvn spring-boot:run -pl producer -Dspring-boot.run.arguments="--rampup.duration.seconds=30 --rampup.end.rate=8000"
```

Quick tips:
- Start `forwarder` (and optional `client`) before the `producer` so events have a consumer path. Forwarder discards events if no clients are registered.

## Metrics and CSV

The forwarder writes a time-series CSV file when it starts. The filename follows the pattern:

`metrics_<MODE>_<timestamp>.csv` (MODE is `REST` or `WEBSOCKET`). 

What is inside
- The CSV file starts with commented metadata lines that include the transport mode and the ramp-up configuration used.
- Every second the `MetricsService` writes one data row (scheduled snapshot). The CSV columns are exactly those emitted by `MetricsService.java`:
  - `TimeSeconds` — seconds since the first received event
  - `Expected_CumulativeSent` — how many events the producer should have emitted (calculated from the configured ramp, so be sure that `forwarder/rampup.properties` matches the producer settings)
  - `Actual_CumulativeKafkaAccepted` — events persisted/accepted by the forwarder
  - `Actual_P50_ms` — 50th percentile latency (ms) for that interval
  - `Actual_P95_ms` — 95th percentile latency (ms) for that interval
  - `Actual_CumulativeAcks` — total ACKs received from clients so far
  - `Actual_Lag_Behind` — `Expected_CumulativeSent - Actual_CumulativeAcks` (positive means the system is falling behind, keep in mind that there is a natural delay between sending and ACKing)

How to interpret
- The `Expected_CumulativeSent` value is synthetic (calculated from the ramp config). Comparing it to `Actual_CumulativeAcks` shows whether the pipeline is keeping up.
- A growing positive `Actual_Lag_Behind` indicates the forwarder/clients/DB cannot keep pace with the producer.
- The forwarder also prints a concise status line to the console every second (`[REST] ACTUAL: ... P95: <ms>`), which is handy during live tests.

Tips
- Ensure `forwarder/rampup.properties` (or the forwarder's application properties) matches the producer ramp settings when you want an accurate expected reference.

## Application properties (locations & quick reference)

Where to find the main properties files:
- `producer/src/main/resources/application.properties` — producer settings (ramp-up parameters)
- `forwarder/src/main/resources/application.properties` — forwarder settings (transport mode (REST/WS), Kafka consumer, DB, tuning)
- `forwarder/src/main/resources/rampup.properties` — forwarder's copy of the ramp configuration used to compute expected emission counts for metrics
- `client/src/main/resources/application.properties` — client settings (transport mode, forwarder URL, registration behavior)

Key settings you will likely change
- Transport modes:
  - `forwarder.transport.mode` and `client.transport.mode` — must be identical (values: `rest` or `websocket`).
- Ramp/load:
  - Producer: `rampup.start.rate`, `rampup.end.rate`, `rampup.duration.seconds`, `rampup.enabled`, `rampup.stop.after` (in `producer/application.properties`)
  - Forwarder: keep `forwarder/rampup.properties` in sync for accurate `Expected_CumulativeSent` in the CSV
- DB:
  - `spring.profiles.active` (in forwarder) — `h2` or `postgres`
- Toxiproxy:
  - Uncomment `spring.profiles.active=toxiproxy` in `client/application.properties` to enable Toxiproxy overrides for testing network faults

How to override at runtime ( you can spawn multiple clients with different ports)
- For example to run client on port 8182:
  - `mvn spring-boot:run -pl client "-Dspring-boot.run.arguments=--server.port=8182 --client.url=http://localhost:8182"`


Recommended quick checklist before a test run
1. Confirm `forwarder.transport.mode` and `client.transport.mode` match.  
2. Confirm `producer` and `forwarder` ramp properties match (or set `rampup.enabled=false` for constant rate tests).  
3. Ensure `docker-compose up -d` started Kafka and (optionally) Toxiproxy if you will use chaos.  
4. Start `forwarder` first, then `client`, then `producer`.  
5. Tail `metrics_*` CSV and forwarder logs to observe P95 and lag.
