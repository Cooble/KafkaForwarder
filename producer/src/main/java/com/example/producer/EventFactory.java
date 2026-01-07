package com.example.producer;

import com.example.common.InternalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.ThreadLocalRandom;

import java.util.concurrent.ThreadLocalRandom;

import java.util.concurrent.ThreadLocalRandom;

public class EventFactory {

    // --- Configuration ---
    private static final int MEAN_SIZE = 1024;  // Target average size
    private static final int DEV_SIZE = 256;    // Standard deviation
    private static final int MIN_SIZE = 50;     // Minimum payload size
    private static final int MAX_SIZE = 4096;   // Maximum payload size

    // We need a large buffer to allow for random offsets + max size.
    // 64KB provides enough variance so messages don't repeat patterns often.
    private static final int MASTER_BUFFER_SIZE = 64 * 1024;

    private static final String MASTER_PAYLOAD;

    // --- Static Initialization ---
    static {
        char[] chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
        char[] buffer = new char[MASTER_BUFFER_SIZE];
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // Fill the 64KB buffer with random noise once
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = chars[rnd.nextInt(chars.length)];
        }
        MASTER_PAYLOAD = new String(buffer);
    }

    /**
     * Generates a random payload with:
     * 1. Random Size (Gaussian distribution around 1kB)
     * 2. Random Content (Random slice from the master buffer)
     * 3. Unique Event ID (timestamp + sequence for guaranteed uniqueness)
     */
    public static InternalData createNaturalEvent(long sequenceId) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // 0. Generate unique event ID
        String eventId = System.currentTimeMillis() + "-" + sequenceId;

        // 1. Calculate Size (Bell Curve)
        int size = (int) (MEAN_SIZE + (random.nextGaussian() * DEV_SIZE));

        // Clamp the size to safe limits
        if (size < MIN_SIZE) size = MIN_SIZE;
        if (size > MAX_SIZE) size = MAX_SIZE;

        // 2. Calculate Random Start Offset
        // We must ensure that (offset + size) does not exceed buffer length.
        // limit = 65536 - 1024 = 64512 valid starting positions
        int maxOffset = MASTER_BUFFER_SIZE - size;
        int offset = random.nextInt(maxOffset);

        // 3. Slice the string
        // This copies the char array range, which is very fast (System.arraycopy underneath)
        String payload = MASTER_PAYLOAD.substring(offset, offset + size);

        return new InternalData(eventId, payload, "event-" + sequenceId);
    }
}