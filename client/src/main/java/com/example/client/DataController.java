package com.example.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
public class DataController {
    @Autowired
    private SimulationService simulationService;

    @PostMapping("/data")
    @ResponseStatus(HttpStatus.OK)
    public void receiveData() {
    }
}
