package com.example.forwarder.db;


import com.example.forwarder.model.ExternalDataTableEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ExternalDataRepository extends JpaRepository<ExternalDataTableEntry, Long> {

    /**
     * Find all existing events by their eventIds - for fetching after idempotent insert.
     */
    @Query("SELECT e FROM ExternalDataTableEntry e WHERE e.eventId IN :eventIds")
    List<ExternalDataTableEntry> findByEventIdIn(@Param("eventIds") List<String> eventIds);

    /**
     * Idempotent insert using MERGE - inserts only if event_id doesn't exist.
     * Works with both H2 and PostgreSQL.
     * Returns number of rows inserted (0 if duplicate).
     */
    @Modifying
    @Query(value = """
        MERGE INTO external_data_table_entry (event_id, topic, document_id, customer_id, currency, total_cents, payload_json, sequence_number, received_at)
        KEY(event_id)
        VALUES (:eventId, :topic, :documentId, :customerId, :currency, :totalCents, :payloadJson, :sequenceNumber, :receivedAt)
        """, nativeQuery = true)
    int mergeOne(@Param("eventId") String eventId,
                 @Param("topic") String topic,
                 @Param("documentId") String documentId,
                 @Param("customerId") String customerId,
                 @Param("currency") String currency,
                 @Param("totalCents") Long totalCents,
                 @Param("payloadJson") String payloadJson,
                 @Param("sequenceNumber") Long sequenceNumber,
                 @Param("receivedAt") java.time.LocalDateTime receivedAt);

    /**
     * Database-native cleanup: Deletes external data that has no delivery statuses.
     * This runs ENTIRELY in the database - no Java memory usage!
     */
    @Modifying
    @Query("""
        DELETE FROM ExternalDataTableEntry e
        WHERE NOT EXISTS (
            SELECT 1 FROM DeliveryStatus d WHERE d.externalDataId = e.id
        )
        """)
    int deleteOrphanedData();
}

