package com.example.forwarder.db;

import com.example.forwarder.model.Client;
import com.example.forwarder.model.ExternalData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DbService {
    @Autowired
    private ExternalDataRepository externalDataRepository;
    @Autowired
    private ClientRepository clientRepository;

    public void saveExternalData(ExternalData data) {
        externalDataRepository.save(data);
    }

    public void saveClient(Client client) {
        clientRepository.save(client);
    }

}
