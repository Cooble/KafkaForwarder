package com.example.forwarder;

import com.example.forwarder.db.DbService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
public class RegistrationController {
    @Autowired
    private DbService dbService;

    // Pass client's IP as identifier, then get clientUrl as parameter string and subscribedTopics as parameter string list
    @PostMapping("/registration")
    @ResponseStatus(HttpStatus.OK)
    public void registerClient(HttpServletRequest request) {
        System.out.println(request.getRemoteAddr());
        dbService.saveClient(new com.example.forwarder.model.Client("client" + (int) (Math.random() * 10000), "http://localhost:8080", null));
    }
    // 1) Figure out how to get user IP / HTTP headers, 2) Figure out how to pass parameters, 3) Make an "unregister" method?


    // A "rest" way of registering and unregistering ???
}
