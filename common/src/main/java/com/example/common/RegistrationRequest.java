package com.example.common;
import java.util.List;

public record RegistrationRequest(String clientUrl, List<String> topics) {
}
