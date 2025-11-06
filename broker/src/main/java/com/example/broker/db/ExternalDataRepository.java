package com.example.broker.db;

import com.example.common.ExternalData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ExternalDataRepository extends JpaRepository<ExternalData, Integer> {
}