package com.example.forwarder;

import com.example.forwarder.db.ClientRepository;
import com.example.forwarder.db.DeliveryStatusRepository;
import com.example.forwarder.db.ExternalDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class DbStatsService {
    private static final Logger log = LoggerFactory.getLogger(DbStatsService.class);

    @Autowired
    private ExternalDataRepository externalDataRepository;

    @Autowired
    private DeliveryStatusRepository deliveryStatusRepository;

    @Autowired
    private ClientRepository clientRepository;

    @Scheduled(fixedRate = 4000)  // Every 5 seconds
    public void logDatabaseStats() {
        long externalDataCount = externalDataRepository.count();
        long deliveryStatusCount = deliveryStatusRepository.count();
        long pendingDeliveries = deliveryStatusRepository.countByConfirmedFalse();
        long clientCount = clientRepository.count();

        log.info("DB Stats: ExternalData={}, DeliveryStatus={} (pending={}), Clients={}",
                externalDataCount, deliveryStatusCount, pendingDeliveries, clientCount);
    }
}

