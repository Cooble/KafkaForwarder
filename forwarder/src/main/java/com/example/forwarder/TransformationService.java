package com.example.forwarder;

import com.example.common.ExternalData;
import com.example.common.InternalData;
import com.example.forwarder.model.ExternalDataTableEntry;
import org.springframework.stereotype.Service;

@Service
public class TransformationService {

    public ExternalDataTableEntry transform(InternalData data, String topic, Long sequenceNumber) {
        return new ExternalDataTableEntry(
                data.eventId(),
                topic,
                data.documentId(),
                data.customerId(),
                data.currency(),
                data.totalCents(),
                data.payloadJson(),
                sequenceNumber
        );
    }

    public ExternalData transform(ExternalDataTableEntry data) {
        return new ExternalData(
                data.getDocumentId(),
                data.getCustomerId(),
                data.getCurrency(),
                data.getTotalCents(),
                data.getPayloadJson()
        );
    }

}
