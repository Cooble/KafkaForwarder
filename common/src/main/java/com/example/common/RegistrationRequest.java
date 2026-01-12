package com.example.common;
import java.util.List;

public record RegistrationRequest(String clientIdentifier, String clientUrl, List<String> topics) {
}
