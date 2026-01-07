package com.example.common;

public record InternalData(
	String documentId,
	String customerId,
	String currency,
	long totalCents,
	String payloadJson
) {}
