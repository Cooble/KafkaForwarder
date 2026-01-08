package com.example.common;

import java.util.List;

public record AckMessage(List<Long> dataIds) {
}

