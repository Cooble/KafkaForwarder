package com.example.forwarder.kafka;

import com.example.forwarder.MetricsService;
import com.example.forwarder.pipeline.ForwarderExecutorService;
import com.example.forwarder.pipeline.ForwarderPipeline;
import com.example.forwarder.pipeline.ForwarderPipeline.KafkaEventInput;
import com.example.forwarder.pipeline.ForwarderPipeline.TransformResult;
import com.example.forwarder.pipeline.ForwarderPipeline.PersistResult;
import com.example.common.InternalData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka listener that feeds events into the ForwarderPipeline.
 * Simplified to only handle Kafka-specific concerns:
 * - Receiving batches from Kafka
 * - Async handoff to pipeline
 * - Offset acknowledgment
 *
 * All processing logic is now in ForwarderPipeline.
 */
@Service
public class KafkaService {
    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);

    @Autowired
    private ForwarderPipeline pipeline;
    @Autowired
    private ForwarderExecutorService executorService;
    @Autowired
    private MetricsService metricsService;

    private final AtomicLong pendingBatches = new AtomicLong(0);

    @KafkaListener(topics = "${kafka.topics}", batch = "true")
    public void listenInternalDataBatch(List<ConsumerRecord<String, InternalData>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            return;
        }

        // Track how many events we received from Kafka
        metricsService.recordKafkaReceived(records.size());

        // Convert Kafka records to pipeline input
        List<KafkaEventInput> inputs = records.stream()
            .map(r -> new KafkaEventInput(r.value(), r.topic(), r.offset()))
            .toList();

        // Process asynchronously - don't block Kafka consumer thread
        pendingBatches.incrementAndGet();
        executorService.submitPipelineTask(() -> processBatch(inputs, acknowledgment))
            .exceptionally(ex -> {
                log.error("Error processing batch of {} records - will NOT commit offset", records.size(), ex);
                pendingBatches.decrementAndGet();
                return null;
            });
    }

    private void processBatch(List<KafkaEventInput> inputs, Acknowledgment acknowledgment) {
        try {
            // Stage 1: Transform (in-memory)
            TransformResult transformResult = pipeline.transformStage(inputs);

            // Stage 2: Persist (DB calls)
            PersistResult persistResult = pipeline.persistStage(transformResult);

            // Stage 3: Dispatch (async sends to clients)
            if (!persistResult.isEmpty()) {
                pipeline.dispatchStage(persistResult.clientBatches());
            }

            // ✅ CRITICAL: Only NOW commit the Kafka offset (data is safely in DB)
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process batch, offset will NOT be committed - Kafka will redeliver", e);
            throw e;
        } finally {
            pendingBatches.decrementAndGet();
        }
    }

    public long getPendingBatches() {
        return pendingBatches.get();
    }
}

