package com.example.common;

public record InternalData(
	String eventId,
	String documentId,
	String customerId,
	String currency,
	long totalCents,
	String payloadJson,
	long bornTimeMs
) {}
