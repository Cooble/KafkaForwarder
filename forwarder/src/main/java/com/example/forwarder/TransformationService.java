package com.example.forwarder;

import com.example.forwarder.db.DbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TransformationService {
    @Autowired
    private WebSocketService webSocketService;
    @Autowired
    private DbService dbService;
}
