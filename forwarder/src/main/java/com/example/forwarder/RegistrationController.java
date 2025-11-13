package com.example.forwarder;

import com.example.common.RegistrationRequest;
import com.example.forwarder.db.DbService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
public class RegistrationController {
    @Autowired
    private DbService dbService;

    // Pass client's IP as identifier, then get clientUrl as parameter string and subscribedTopics as parameter string list
    @PostMapping("/registration")
    @ResponseStatus(HttpStatus.OK)
    public void registerClient(HttpServletRequest request, @RequestBody RegistrationRequest registrationRequest) {
        System.out.println("Received registration request: " +  registrationRequest);
        dbService.saveClient(new com.example.forwarder.model.Client(request.getRemoteAddr(), registrationRequest.clientUrl(), registrationRequest.topics()));
    }
    // 2) Figure out how to pass parameters, 3) Make an "unregister" method?


    // A "rest" way of registering and unregistering ???
}
