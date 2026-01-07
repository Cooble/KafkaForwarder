package com.example.forwarder.db;


import com.example.forwarder.model.ExternalDataTableEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface ExternalDataRepository extends JpaRepository<ExternalDataTableEntry, Long> {

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

