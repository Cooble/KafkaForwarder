# Metrics Implementation for End-to-End Latency Measurement

## Overview
This implementation measures the **end-to-end latency** from when the forwarder receives a Kafka event until the client confirms receipt (via HTTP 200 response).

## What Was Implemented

### 1. Birth Timestamp Tracking in DeliveryStatus
Added a `bornTimeMs` field to the `DeliveryStatus` entity:
- **Type**: `long` (milliseconds since epoch)
- **Purpose**: Records when the Kafka event was received by the forwarder
- **Set at**: The moment the `KafkaListener` receives the event from Kafka

### 2. Flow of Timestamp Through the System

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. Kafka Event Arrives                                              │
│    KafkaService.listenInternalData()                                │
│    └─> bornTimeMs = System.currentTimeMillis()                      │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 2. Create DeliveryStatus for Each Client                            │
│    DbService.createDeliveryStatuses(externalDataId, clientIds,      │
│                                     bornTimeMs)                      │
│    └─> Each DeliveryStatus stores the bornTimeMs                    │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 3. Send to Client (Async)                                           │
│    SendService.sendToClient()                                       │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│ 4. Client Confirms Receipt (HTTP 200)                               │
│    DeliveryResultHandler.handleSendResult()                         │
│    └─> metricsService.recordAck(status.getBornTimeMs())             │
│        └─> Calculates: latency = now - bornTimeMs                   │
└─────────────────────────────────────────────────────────────────────┘
```

### 3. Metrics Collection
The `MetricsService` uses **HdrHistogram** for high-performance latency recording:
- **Throughput**: Messages acknowledged per second
- **Latency Percentiles**: Mean, P99, Max
- **Output**: CSV file `broker_stats_<timestamp>.csv`

## CSV Output Format
```csv
TimeSeconds,Throughput_MsgPerSec,Mean_ms,P99_ms,Max_ms
0,1523,45.23,120,450
1,1587,47.12,125,380
2,1605,46.89,118,350
...
```

## What This Measures

### End-to-End External Event Flow
✅ **Measured**: Time from forwarder receiving Kafka event → client confirming receipt
- Includes: DB writes, delivery attempts, network latency, client processing
- Excludes: Kafka internal processing (as requested)

### Per-Client Metrics
Each client gets its own `DeliveryStatus` with the same `bornTimeMs`, so:
- Multiple clients subscribing to the same topic will each contribute to metrics
- You can see how latency changes with more concurrent clients

## How to Generate Nice Graphs

### 1. Current Implementation (Constant Rate)
The current producer likely sends at a constant rate. The CSV can be visualized with:

**Python Example:**
```python
import pandas as pd
import matplotlib.pyplot as plt

# Read CSV
df = pd.read_csv('broker_stats_<timestamp>.csv')

# Plot Latency vs Throughput
plt.figure(figsize=(12, 6))
plt.subplot(1, 2, 1)
plt.plot(df['TimeSeconds'], df['P99_ms'], label='P99 Latency')
plt.plot(df['TimeSeconds'], df['Mean_ms'], label='Mean Latency')
plt.xlabel('Time (seconds)')
plt.ylabel('Latency (ms)')
plt.legend()
plt.title('Latency Over Time')

plt.subplot(1, 2, 2)
plt.plot(df['TimeSeconds'], df['Throughput_MsgPerSec'])
plt.xlabel('Time (seconds)')
plt.ylabel('Messages/sec')
plt.title('Throughput Over Time')
plt.tight_layout()
plt.savefig('metrics.png')
```

### 2. Proposed: Exponential Distribution for Events

To simulate more realistic traffic patterns with bursty behavior:

**Update Producer to use Exponential Distribution:**
```java
import java.util.Random;

public class ExponentialRateProducer {
    private final Random random = new Random();
    private final double lambda; // Rate parameter (events per second)
    
    public ExponentialRateProducer(double averageRatePerSecond) {
        this.lambda = averageRatePerSecond;
    }
    
    // Returns delay in milliseconds until next event
    public long nextEventDelay() {
        // Exponential distribution: -ln(1-U)/λ where U ~ Uniform(0,1)
        double u = random.nextDouble();
        double delaySeconds = -Math.log(1 - u) / lambda;
        return (long)(delaySeconds * 1000);
    }
    
    public void produceWithExponentialRate() throws InterruptedException {
        while (true) {
            // Send event
            sendEvent();
            
            // Wait exponentially distributed time
            long delay = nextEventDelay();
            Thread.sleep(delay);
        }
    }
}
```

### 3. Ramping Load Test

To show "how latency increases as Kafka events per second rises":

**Gradual Load Increase:**
```java
public class RampingLoadTest {
    public void runRampingTest(int durationSeconds, int startRate, int endRate) {
        double currentRate = startRate;
        double rateIncrement = (double)(endRate - startRate) / durationSeconds;
        
        for (int second = 0; second < durationSeconds; second++) {
            ExponentialRateProducer producer = new ExponentialRateProducer(currentRate);
            
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 1000) {
                producer.sendEvent();
                Thread.sleep(producer.nextEventDelay());
            }
            
            currentRate += rateIncrement;
            System.out.println("Second " + second + ": Rate = " + currentRate + "/sec");
        }
    }
}
```

**Expected Graph:**
```
Latency (ms)
    ^
    |                                    ╱
800 |                                  ╱
    |                                ╱
600 |                              ╱
    |                            ╱
400 |                         ╱
    |                      ╱
200 |                 ╱
    |           ╱╱╱╱
  0 |___╱╱╱╱╱╱─────────────────────────> Throughput (msg/sec)
    0   100  200  300  400  500  600
    
    Shows system breaking point where latency spikes
```

## Database Schema Update

The `bornTimeMs` field is automatically added to the database schema:
```sql
ALTER TABLE delivery_status 
ADD COLUMN born_time_ms BIGINT NOT NULL;
```

## Next Steps for Better Metrics

1. **Ramp-Up Producer**: Modify producer to gradually increase rate (10 → 1000 msg/sec over 5 minutes)
2. **Exponential Inter-Arrival**: Use exponential distribution for more realistic traffic
3. **Visualization**: Create Python script to generate graphs from CSV
4. **Breaking Point Analysis**: Find the throughput where P99 latency exceeds SLA (e.g., 500ms)
5. **Multi-Client Testing**: Test with 1, 5, 10, 20 concurrent clients to see scalability

## Key Metrics to Analyze

- **Throughput Ceiling**: Max msg/sec before latency degrades
- **Latency SLA**: What percentage of messages meet <100ms, <500ms targets?
- **Retry Impact**: How does retry logic affect latency distribution?
- **Client Count Effect**: Does latency increase linearly with client count?

