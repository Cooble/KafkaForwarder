package com.example.forwarder.db;

import com.example.forwarder.model.DeliveryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DeliveryStatusRepository extends JpaRepository<DeliveryStatus, Long> {

    // ========== NATIVE SQL QUERIES (No Hibernate Magic!) ==========

    /**
     * Count pending (unconfirmed) deliveries - returns count, NOT 300K objects!
     */
    @Query("SELECT COUNT(d) FROM DeliveryStatus d WHERE d.confirmed = false")
    long countPending();


    /**
     * Get pending deliveries that are older than the cutoff time (grace period).
     * Only returns entries where bornTimeMs < cutoffTimeMs to avoid retrying events
     * that are still awaiting ACK processing.
     * Ordered by bornTimeMs (oldest first) and clientId for efficient batching.
     */
    @Query("SELECT d FROM DeliveryStatus d WHERE d.confirmed = false AND d.bornTimeMs < :cutoffTimeMs ORDER BY d.bornTimeMs, d.clientId")
    List<DeliveryStatus> findPendingForRetry(@Param("cutoffTimeMs") long cutoffTimeMs);

    /**
     * Bulk update - marks multiple delivery statuses as confirmed in a single UPDATE statement.
     * This is MUCH faster than fetching, modifying, and saving individual entities.
     */
    @Modifying
    @Query("UPDATE DeliveryStatus d SET d.confirmed = true WHERE d.externalDataId IN :dataIds AND d.clientId IN :clientIds AND d.confirmed = false")
    int markAsConfirmedBulk(@Param("dataIds") List<Long> dataIds, @Param("clientIds") List<Long> clientIds);

    /**
     * Bulk delete by data IDs - single DELETE statement.
     */
    @Modifying
    @Query("DELETE FROM DeliveryStatus d WHERE d.externalDataId IN :dataIds")
    int deleteByDataIds(@Param("dataIds") List<Long> dataIds);


    /**
     * Database-native cleanup: Deletes delivery statuses for data IDs where ALL statuses are confirmed.
     * Optimized to use NOT EXISTS instead of GROUP BY for better performance.
     * This runs ENTIRELY in the database - no Java memory usage!
     */
    @Modifying
    @Query("""
        DELETE FROM DeliveryStatus d 
        WHERE d.confirmed = true
        AND NOT EXISTS (
            SELECT 1 
            FROM DeliveryStatus ds 
            WHERE ds.externalDataId = d.externalDataId 
            AND ds.confirmed = false
        )
        """)
    int deleteFullyConfirmedDeliveryStatuses();
}

