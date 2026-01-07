package com.example.forwarder.db;

import com.example.forwarder.model.DeliveryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DeliveryStatusRepository extends JpaRepository<DeliveryStatus, Long> {
    Optional<DeliveryStatus> findByExternalDataIdAndClientId(Long externalDataId, Long clientId);

    List<DeliveryStatus> findByConfirmedFalse();

    long countByConfirmedFalse();

    long countByExternalDataIdAndConfirmedTrue(Long externalDataId);

    long countByExternalDataId(Long externalDataId);

    List<DeliveryStatus> findByExternalDataId(Long externalDataId);

    void deleteByExternalDataId(Long externalDataId);

    void deleteByExternalDataIdIn(List<Long> externalDataIds);
}

