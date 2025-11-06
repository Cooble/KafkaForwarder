package com.example.broker;

import com.example.broker.db.DbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TransformationService {
    @Autowired
    private SendService sendService;
    @Autowired
    private DbService dbService;
}
