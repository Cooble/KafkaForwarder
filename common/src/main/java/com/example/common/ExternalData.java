package com.example.common;

public record ExternalData(
	String documentId,
	String customerId,
	String currency,
	long totalCents,
	String payloadJson
) {}