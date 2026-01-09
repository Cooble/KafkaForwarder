package com.example.common;

/**
 * Configuration and calculation logic for ramp-up behavior.
 * Used by both the producer (to emit events) and forwarder (to calculate expected metrics).
 */
public class RampupConfig {

    private final boolean enabled;
    private final double startRate;
    private final double endRate;
    private final int durationSeconds;
    private final boolean stopAfter;

    public RampupConfig(boolean enabled, double startRate, double endRate, int durationSeconds, boolean stopAfter) {
        this.enabled = enabled;
        this.startRate = startRate;
        this.endRate = endRate;
        this.durationSeconds = durationSeconds;
        this.stopAfter = stopAfter;
    }

    // === Getters ===

    public boolean isEnabled() {
        return enabled;
    }

    public double getStartRate() {
        return startRate;
    }

    public double getEndRate() {
        return endRate;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public long getDurationMs() {
        return durationSeconds * 1000L;
    }

    public boolean isStopAfter() {
        return stopAfter;
    }

    // === Calculation Methods ===

    /**
     * Check if the ramp-up period is complete.
     */
    public boolean isRampComplete(long elapsedMs) {
        return elapsedMs >= getDurationMs();
    }

    /**
     * Check if the producer should stop (ramp complete and stopAfter is true).
     */
    public boolean shouldStop(long elapsedMs) {
        return enabled && stopAfter && isRampComplete(elapsedMs);
    }

    /**
     * Calculate the current emission rate at a given elapsed time.
     *
     * @param elapsedMs milliseconds since start
     * @return current rate in events per second
     */
    public double getCurrentRate(long elapsedMs) {
        if (!enabled) {
            // Constant rate mode
            return startRate;
        }

        if (isRampComplete(elapsedMs)) {
            // Ramp complete - return end rate or 0 if stopping
            return stopAfter ? 0 : endRate;
        }

        // Linear ramp: rate = startRate + (progress * range)
        double progress = (double) elapsedMs / getDurationMs();
        double rateRange = endRate - startRate;
        return startRate + (progress * rateRange);
    }

    /**
     * Calculate the expected cumulative events sent up to a given elapsed time.
     * This is the integral of the rate function.
     *
     * @param elapsedMs milliseconds since start
     * @return expected total events sent
     */
    public long getCumulativeEvents(long elapsedMs) {
        if (!enabled) {
            // Constant rate: events = rate * time
            double elapsedSeconds = elapsedMs / 1000.0;
            return (long) (startRate * elapsedSeconds);
        }

        if (isRampComplete(elapsedMs)) {
            // Calculate total area under ramp (trapezoid)
            double rampArea = (startRate + endRate) / 2.0 * durationSeconds;

            if (stopAfter) {
                // Producer stops after ramp - no additional messages
                return (long) rampArea;
            } else {
                // Producer continues at end rate - add constant rate * time after ramp
                double timeAfterRampSeconds = (elapsedMs - getDurationMs()) / 1000.0;
                return (long) (rampArea + (endRate * timeAfterRampSeconds));
            }
        }

        // During ramp: integral from 0 to current time
        // For linear ramp r(t) = startRate + (t/T) * (endRate - startRate)
        // Integral = startRate * t + (1/2) * (t^2/T) * (endRate - startRate)
        double t = elapsedMs / 1000.0;
        double T = durationSeconds;
        double rateRange = endRate - startRate;
        return (long) (startRate * t + 0.5 * (t * t / T) * rateRange);
    }

    @Override
    public String toString() {
        if (enabled) {
            return String.format("RampupConfig[%s -> %s events/sec over %d sec, stopAfter=%s]",
                    (int) startRate, (int) endRate, durationSeconds, stopAfter);
        } else {
            return String.format("RampupConfig[constant %s events/sec]", (int) startRate);
        }
    }
}

