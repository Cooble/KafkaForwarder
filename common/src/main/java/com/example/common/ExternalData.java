package com.example.common;

public record ExternalData(
	Long id,
	String documentId,
	String customerId,
	String currency,
	long totalCents,
	String payloadJson
) {}