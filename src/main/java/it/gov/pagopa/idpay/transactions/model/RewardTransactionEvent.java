package it.gov.pagopa.idpay.transactions.model;

import java.time.OffsetDateTime;

public record RewardTransactionEvent(
        String eventId,
        int schemaVersion,
        String eventType,
        OffsetDateTime occurredAt,
        long transactionRevision,
        RewardTransaction transaction
) {
}
