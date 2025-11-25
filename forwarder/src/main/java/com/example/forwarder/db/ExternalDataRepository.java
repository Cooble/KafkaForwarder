package com.example.forwarder.db;


import com.example.forwarder.model.ExternalDataTableEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ExternalDataRepository extends JpaRepository<ExternalDataTableEntry, Long> {

}