package com.example.broker;

import com.example.broker.db.DbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
public class RegistrationController {
    @Autowired
    private DbService dbService;

    @PostMapping("/registration")
    @ResponseStatus(HttpStatus.OK)
    public void registerClient() {}
}
