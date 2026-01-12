package com.example.forwarder;

import com.example.forwarder.db.ClientRepository;
import com.example.forwarder.db.DbService;
import com.example.forwarder.db.DeliveryStatusRepository;
import com.example.forwarder.db.ExternalDataRepository;
import com.example.forwarder.pipeline.ForwarderExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Logs database statistics and performs periodic cleanup.
 * Uses ForwarderExecutorService for async cleanup operations.
 */
@Service
public class DbStatsService {
    private static final Logger log = LoggerFactory.getLogger(DbStatsService.class);

    @Autowired
    private DbService dbService;

    @Autowired
    private ExternalDataRepository externalDataRepository;

    @Autowired
    private DeliveryStatusRepository deliveryStatusRepository;

    @Autowired
    private ClientRepository clientRepository;

    @Autowired
    private ForwarderExecutorService executorService;

    // Prevent concurrent cleanup runs
    private final AtomicBoolean cleanupInProgress = new AtomicBoolean(false);

    @Scheduled(fixedRate = 4000)  // Every 4 seconds
    public void logDatabaseStats() {
        long externalDataCount = externalDataRepository.count();
        long deliveryStatusCount = deliveryStatusRepository.count();
        long pendingDeliveries = deliveryStatusRepository.countPending();
        long clientCount = clientRepository.count();

        //log.info("DB Stats: ExternalData={}, DeliveryStatus={} (pending={}), Clients={}",
       //         externalDataCount, deliveryStatusCount, pendingDeliveries, clientCount);
    }

    /**
     * Cleanup task - deletes fully confirmed data entries using pure SQL.
     * Runs entirely in the database - no Java memory overhead!
     * Now async to prevent blocking scheduler thread during heavy cleanup.
     */
    @Scheduled(fixedRate = 5000)  // Every 5 seconds
    public void cleanupConfirmedData() {
        // Skip if cleanup is already running - prevent queue buildup
        if (!cleanupInProgress.compareAndSet(false, true)) {
            log.debug("Cleanup already in progress, skipping this cycle");
            return;
        }

        // Run cleanup asynchronously using centralized executor
        executorService.submitCleanupTask(() -> {
            long startTime = System.currentTimeMillis();
            try {
                DbService.CleanupResult result = dbService.cleanupConfirmedDataAndOrphans();

                long duration = System.currentTimeMillis() - startTime;
                if (result.deletedStatuses() > 0 || result.deletedData() > 0) {
                    log.info("Cleanup: Deleted {} delivery statuses, {} data entries in {}ms",
                        result.deletedStatuses(), result.deletedData(), duration);
                }

                if (duration > 2000) {
                    log.warn("Slow cleanup operation took {}ms - consider optimizing DB indexes", duration);
                }
            } catch (Exception e) {
                log.error("Cleanup task failed: {}", e.getMessage(), e);
            } finally {
                cleanupInProgress.set(false);
            }
        });
    }
}

