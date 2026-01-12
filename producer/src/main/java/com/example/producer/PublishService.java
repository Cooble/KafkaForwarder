package com.example.producer;

import com.example.common.InternalData;
import com.example.common.RampupConfig;
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

    @Value("${rampup.enabled:true}")
    private boolean rampEnabled;

    @Value("${rampup.start.rate:1700}")
    private double startRate;

    @Value("${rampup.end.rate:3000}")
    private double endRate;

    @Value("${rampup.duration.seconds:60}")
    private int rampDurationSeconds;

    @Value("${rampup.stop.after:true}")
    private boolean stopAfterRamp;

    @Autowired
    private KafkaTemplate<String, InternalData> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(PublishService.class);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private int count = 0;
    private RampupConfig rampupConfig;

    @EventListener(ApplicationReadyEvent.class)
    public void startSpamming() {
        rampupConfig = new RampupConfig(rampEnabled, startRate, endRate, rampDurationSeconds, stopAfterRamp);
        log.info("Starting spammer with config: {}", rampupConfig);

        Thread spamThread = new Thread(this::spamLoop);
        spamThread.setName("Poisson-Spammer-Thread");
        spamThread.setDaemon(true);
        spamThread.start();
    }

    private void spamLoop() {
        long startTimeMs = System.currentTimeMillis();

        // All timing uses NANOS for precision
        long nextTargetTimeNs = System.nanoTime();
        long lastLogTimeNs = System.nanoTime();
        long lastRateLogMs = startTimeMs;

        while (running.get()) {
            long elapsedMs = System.currentTimeMillis() - startTimeMs;

            // 1. Check if we should stop
            if (rampupConfig.shouldStop(elapsedMs)) {
                log.info("Sent {} events in total during ramp-up.", count);
                log.info("Ramp-up complete and stopAfter is true. Stopping spammer.");
                break;
            }

            // 2. Calculate current rate using shared config
            double currentRate = rampupConfig.getCurrentRate(elapsedMs);

            // 3. Send the message
            sendMessage(EventFactory.createNaturalEvent(count++));

            // 4. Calculate Poisson delay based on current rate (in NANOS)
            double meanIntervalNs = 1_000_000_000.0 / currentRate;
            double u = ThreadLocalRandom.current().nextDouble();
            double delayNs = -meanIntervalNs * Math.log(1.0 - u);

            // 5. Advance the target time (NANOS)
            nextTargetTimeNs += (long) delayNs;

            // 6. Busy-wait until next event time (NANOS)
            while (System.nanoTime() < nextTargetTimeNs)
                Thread.onSpinWait();

            // 7. Logging (using NANOS for high frequency log check)
            long nowNs = System.nanoTime();
            if (nowNs - lastLogTimeNs > 1_000_000_000L) { // Every 1 second
                log.info("Sent {} events so far", count);
                lastLogTimeNs = nowNs;
            }

            // Log current rate every 5 seconds during ramp-up (using MILLIS for human-readable timestamps)
            if (rampupConfig.isEnabled()) {
                long nowMs = System.currentTimeMillis();
                if (nowMs - lastRateLogMs > 5000) {
                    long elapsedSec = elapsedMs / 1000;
                    log.info("Ramp-up progress: {}/{} seconds, Current rate: {} events/sec",
                            elapsedSec, rampupConfig.getDurationSeconds(), (int) currentRate);
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
