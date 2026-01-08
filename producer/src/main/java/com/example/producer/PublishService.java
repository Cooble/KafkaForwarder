package com.example.producer;

import com.example.common.InternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class PublishService {

    @Value("${producer.ramp.enabled:true}")
    private boolean rampEnabled;

    @Value("${producer.ramp.start.rate:1700}")
    private double startRate;

    @Value("${producer.ramp.end.rate:3000}")
    private double endRate;

    @Value("${producer.ramp.duration.seconds:60}")
    private int rampDurationSeconds;

    @Value( "${producer.ramp.stop.after:true}")
    private boolean stopAfterRamp;

    @Autowired
    private KafkaTemplate<String, InternalData> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(PublishService.class);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private int count = 0;

    @EventListener(ApplicationReadyEvent.class)
    public void startSpamming() {
        if (rampEnabled) {
            System.out.println("Starting ramp-up spammer: " + startRate + " -> " + endRate +
                             " events/sec over " + rampDurationSeconds + " seconds");
        } else {
            System.out.println("Starting constant rate spammer at " + startRate + " events/sec");
        }

        Thread spamThread = new Thread(this::spamLoop);
        spamThread.setName("Poisson-Spammer-Thread");
        spamThread.setDaemon(true);
        spamThread.start();
    }

    private void spamLoop() {
        long startTimeMs = System.currentTimeMillis();
        long rampDurationMs = rampDurationSeconds * 1000L;
        double rateRange = endRate - startRate;

        System.out.println("Starting Poisson spammer with " +
                         (rampEnabled ? "ramp-up" : "constant rate"));


        // All timing uses NANOS for precision
        long nextTargetTimeNs = System.nanoTime();
        long lastLogTimeNs = System.nanoTime();
        long lastRateLogMs = startTimeMs;

        while (running.get()) {
            // 1. Calculate current rate (ramp-up or constant)
            double currentRate;
            if (rampEnabled) {
                long elapsedMs = System.currentTimeMillis() - startTimeMs;
                if (elapsedMs >= rampDurationMs) {
                    // Ramp complete, stay at end rate
                    currentRate = endRate;
                } else {
                    // Linear ramp: rate = startRate + (progress * range)
                    double progress = (double) elapsedMs / rampDurationMs;
                    currentRate = startRate + (progress * rateRange);
                }
            } else {
                currentRate = startRate;
            }

            // 2. Send the message
            sendMessage(EventFactory.createNaturalEvent(count++));
            // If ramp is complete and configured to stop, exit loop
            if (rampEnabled && stopAfterRamp) {
                long elapsedMs = System.currentTimeMillis() - startTimeMs;
                if (elapsedMs >= rampDurationMs) {
                    log.info("Sent {} events in total during ramp-up.", count);
                    log.info("Ramp-up complete and stopAfterRamp is true. Stopping spammer.");
                    break;
                }
            }

            // 3. Calculate Poisson delay based on current rate (in NANOS)
            double meanIntervalNs = 1_000_000_000.0 / currentRate;
            double u = ThreadLocalRandom.current().nextDouble();
            double delayNs = -meanIntervalNs * Math.log(1.0 - u);

            // 4. Advance the target time (NANOS)
            nextTargetTimeNs += (long) delayNs;

            // 5. Busy-wait until next event time (NANOS)
            while (System.nanoTime() < nextTargetTimeNs)
                Thread.onSpinWait();

            // 6. Logging (using NANOS for high frequency log check)
            long nowNs = System.nanoTime();
            if (nowNs - lastLogTimeNs > 1_000_000_000L) { // Every 1 second
                log.info("Sent {} events so far", count);
                lastLogTimeNs = nowNs;
            }

            // Log current rate every 5 seconds during ramp-up (using MILLIS for human-readable timestamps)
            if (rampEnabled) {
                long nowMs = System.currentTimeMillis();
                if (nowMs - lastRateLogMs > 5000) {
                    long elapsedSec = (nowMs - startTimeMs) / 1000;
                    log.info("Ramp-up progress: {}/{} seconds, Current rate: {} events/sec",
                            elapsedSec, rampDurationSeconds, (int)currentRate);
                    lastRateLogMs = nowMs;
                }
            }
        }
    }

    public void stop() {
        running.set(false);
    }

    public void sendMessage(InternalData data) {
        kafkaTemplate.send("topic1", data);
    }
}
