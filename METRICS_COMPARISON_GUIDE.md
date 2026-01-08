pls# REST vs WebSocket Metrics Comparison Guide

## Overview

This guide explains how to run performance tests for both REST and WebSocket modes and compare the results with nice graphs.

## Enhanced Metrics Collection

The `MetricsService` has been upgraded to collect comprehensive metrics:

### New Metrics Tracked:
- **Transport Mode** (REST/WEBSOCKET) - labeled in filename and CSV
- **Throughput** - ACKs per second
- **Send Attempts** - How many messages were attempted
- **Failures** - How many sends failed
- **Success Rate** - Percentage of successful sends
- **Latency Percentiles** - P50, P95, P99, P99.9, Max
- **Cumulative Counters** - Total ACKs, expected sent, failures

### CSV Output Format:
```csv
TimeSeconds,TransportMode,EmissionRate_PerSec,Throughput_MsgPerSec,SendAttempts_PerSec,Failed_PerSec,CumulativeAcks,CumulativeSent,CumulativeFailed,SuccessRate_%,Mean_ms,P50_ms,P95_ms,P99_ms,P999_ms,Max_ms,StartRate,EndRate,Duration
```

### Output Files:
- **REST mode**: `metrics_REST_<timestamp>.csv`
- **WebSocket mode**: `metrics_WEBSOCKET_<timestamp>.csv`

## Step-by-Step Comparison Test

### Prerequisites

1. **Install Python dependencies** (for graph generation):
   ```bash
   pip install pandas matplotlib seaborn
   ```

2. **Ensure all services are ready**:
   - Kafka running (docker-compose up)
   - Database ready (H2 or PostgreSQL)

### Step 1: Run REST Mode Test

1. **Configure REST mode** in `forwarder/src/main/resources/application.properties`:
   ```properties
   forwarder.transport.mode=rest
   ```

2. **Start the forwarder**:
   ```bash
   cd forwarder
   mvn spring-boot:run
   ```
   
   You'll see: `📊 Metrics file created: metrics_REST_<timestamp>.csv (Mode: REST)`

3. **Start the client** (in another terminal):
   ```bash
   cd client
   mvn spring-boot:run
   ```

4. **Start the producer** (in another terminal):
   ```bash
   cd producer
   mvn spring-boot:run
   ```

5. **Wait for test completion** (5-10 minutes depending on your ramp settings)
   - Watch the console output: `[REST] Stats: ...`
   - The CSV file is updated every second

6. **Stop all services** (Ctrl+C in each terminal)

### Step 2: Run WebSocket Mode Test

1. **Configure WebSocket mode** in `forwarder/src/main/resources/application.properties`:
   ```properties
   forwarder.transport.mode=websocket
   ```

2. **Clean the database** (optional, for fair comparison):
   ```bash
   # If using H2, just restart
   # If using PostgreSQL, truncate tables or drop/recreate database
   ```

3. **Repeat steps 2-6 from REST mode**
   
   You'll see: `📊 Metrics file created: metrics_WEBSOCKET_<timestamp>.csv (Mode: WEBSOCKET)`
   
   Console output: `[WEBSOCKET] Stats: ...`

### Step 3: Generate Comparison Graphs

1. **Navigate to forwarder directory**:
   ```bash
   cd forwarder
   ```

2. **Run the graph generator**:
   ```bash
   python generate_comparison_graphs.py
   ```
   
   The script will:
   - Auto-detect the most recent `metrics_REST_*.csv` and `metrics_WEBSOCKET_*.csv`
   - Generate comprehensive comparison graphs
   - Save them in the `graphs/` directory

3. **View the results**:
   - `graphs/rest_vs_websocket_comparison.png` - Main 6-panel comparison
   - `graphs/latency_distribution_boxplot.png` - Latency distribution
   - `graphs/summary_statistics.png` - Summary table

### Manual File Selection

If you have multiple test runs, specify files manually:

```bash
python generate_comparison_graphs.py metrics_REST_1234567890.csv metrics_WEBSOCKET_9876543210.csv
```

## Generated Graphs Explained

### 1. Main Comparison (6 panels)

**Top Left - Throughput Over Time:**
- Shows ACKs/second for REST (blue) vs WebSocket (green)
- Red dashed line = target emission rate
- **Look for**: Which mode keeps up with target rate better

**Top Right - P99 Latency:**
- 99th percentile latency over time
- **Look for**: Which mode has lower/more stable latency

**Middle Left - Cumulative Messages:**
- Total delivered messages vs expected
- **Look for**: Which mode delivers more messages total

**Middle Right - Success Rate:**
- Percentage of successful sends
- **Look for**: Which mode has higher success rate

**Bottom Left - Latency Percentiles:**
- P50, P95, P99 for both modes
- **Look for**: Full latency profile comparison

**Bottom Right - Mean Latency:**
- Average latency over time
- **Look for**: Which mode has lower average latency

### 2. Latency Distribution Boxplot

Shows the statistical distribution of latencies:
- **Box**: 25th to 75th percentile (middle 50%)
- **Line in box**: Median
- **Whiskers**: Min/Max (excluding outliers)
- **Look for**: Which mode has tighter distribution (smaller box)

### 3. Summary Statistics Table

Quick comparison of key metrics:
- Average & max throughput
- Average & max P99 latency
- Total ACKs delivered
- Success rate
- **Winner column**: Which mode performed better

## What to Look For

### Expected WebSocket Advantages:
✅ **Lower latency** - No HTTP overhead, persistent connection
✅ **More stable latency** - No connection establishment per request
✅ **Higher throughput at low client counts** - Less protocol overhead

### Expected REST Advantages:
✅ **Better scalability** - Connection pool handles many clients
✅ **More predictable** - Mature HTTP stack
✅ **Better under connection failures** - Stateless, easier retry

### Concerning Signs:
⚠️ **Success rate < 95%** - Something is wrong with capacity
⚠️ **Cumulative ACKs << Expected** - Messages being dropped
⚠️ **P99 latency > 1000ms** - System is overloaded
⚠️ **Throughput far below emission rate** - Bottleneck in pipeline

## Performance Tuning

### If REST is slow:
1. Increase `forwarder.client.max.concurrent.requests` (default: 500)
2. Increase `forwarder.client.max.batch.size` (default: 100)
3. Tune WebClient connection pool

### If WebSocket is slow:
1. Check per-session queue depth in logs
2. Increase queue size in `WebSocketSessionSender` (default: 10,000)
3. Check if sender threads are keeping up

### If both are slow:
1. Database is likely the bottleneck
2. Increase DB connection pool (`spring.datasource.hikari.maximumPoolSize`)
3. Increase Kafka consumer threads (`spring.kafka.listener.concurrency`)
4. Check `kafka.batch.size` configuration

## Example Test Scenarios

### Scenario 1: High Load Test
```properties
producer.ramp.start.rate=100
producer.ramp.end.rate=5000
producer.ramp.duration.seconds=300
```
Tests how each mode handles increasing load.

### Scenario 2: Sustained High Throughput
```properties
producer.ramp.start.rate=3000
producer.ramp.end.rate=3000
producer.ramp.duration.seconds=600
```
Tests sustained performance at high rate.

### Scenario 3: Spike Test
```properties
producer.ramp.start.rate=100
producer.ramp.end.rate=10000
producer.ramp.duration.seconds=60
```
Tests how quickly each mode adapts to sudden load increase.

## Troubleshooting

### No metrics file created:
- Check that `MetricsService` is being initialized
- Check for errors in forwarder startup logs

### Graph generation fails:
```bash
# Install dependencies
pip install pandas matplotlib seaborn

# Check CSV file format
head -n 5 metrics_REST_*.csv
```

### Graphs show weird data:
- Ensure both tests ran for similar duration
- Check that ramp parameters were the same
- Verify database was clean between tests

### Performance is bad for both modes:
- Check Kafka is running: `docker ps`
- Check DB connection pool: Look for "HikariPool" warnings
- Check for CPU/memory constraints: `top` or Task Manager

## Metrics in Code

If you want to track custom metrics, use `MetricsService`:

```java
@Autowired
private MetricsService metricsService;

// Track successful send attempt
metricsService.recordSendAttempt(batchSize);

// Track failed send
metricsService.recordSendFailure(batchSize);

// Track ACK (already done in DeliveryResultHandler)
metricsService.recordAck(ingestTimeMs);
```

## Interpreting Results

### WebSocket Should Win If:
- You have **few clients** (< 100)
- You need **low latency** (< 10ms)
- Network is **stable**
- Messages are **small to medium** size

### REST Should Win If:
- You have **many clients** (> 100)
- Clients are **behind firewalls/NATs**
- Network is **unreliable**
- You need **horizontal scaling**

### They're Equal If:
- Database is the bottleneck (both limited by same DB throughput)
- Network latency dominates (both see same network delay)
- Load is very low (both handle it easily)

## Next Steps

After comparing:

1. **Document your findings** - Add results to project README
2. **Choose the best mode** - Update default in `application.properties`
3. **Optimize the winner** - Tune parameters for production
4. **Set up monitoring** - Use Prometheus/Grafana for production metrics

## Advanced: Real-time Monitoring

For live monitoring during tests:

```bash
# Watch metrics in real-time
tail -f metrics_REST_*.csv

# Or use a CSV viewer that auto-refreshes
# Import into Excel/LibreOffice and enable auto-update
```

## Questions to Answer

After running both tests, you should be able to answer:

1. **Which mode has better throughput?**
2. **Which mode has lower latency?**
3. **Which mode is more stable under load?**
4. **At what load level do they start to differ?**
5. **Which mode matches your production needs?**

Happy benchmarking! 📊🚀

