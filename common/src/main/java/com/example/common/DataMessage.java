package com.example.common;

import java.util.List;

/**
 * Wrapper for data sent to clients with IDs for acknowledgment.
 */
public record DataMessage(List<Long> dataIds, List<ExternalData> data) {
}

