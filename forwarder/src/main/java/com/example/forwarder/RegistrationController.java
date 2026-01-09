package com.example.forwarder;

import com.example.common.RegistrationRequest;
import com.example.forwarder.model.Client;
import com.example.forwarder.registry.ClientRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
public class RegistrationController {
    @Autowired
    private ClientRegistry clientRegistry;

    private static final Logger log = LoggerFactory.getLogger(RegistrationController.class);

    @PostMapping("/registration")
    @ResponseStatus(HttpStatus.OK)
    public void registerClient(HttpServletRequest request, @RequestBody RegistrationRequest registrationRequest) {
        String clientIp = request.getRemoteAddr();

        clientRegistry.registerClient(new Client(clientIp, registrationRequest.clientUrl(), registrationRequest.topics()));
    }

    // TODO: Make an "unregister" method? or maybe screw it, its a demo


}
