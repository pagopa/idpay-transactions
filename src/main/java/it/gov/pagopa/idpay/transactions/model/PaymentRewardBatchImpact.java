package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.enums.PaymentRewardBatchImpactType;
import java.time.OffsetDateTime;

public record PaymentRewardBatchImpact(
        String eventId,
        int schemaVersion,
        PaymentRewardBatchImpactType impactType,
        OffsetDateTime occurredAt,
        long transactionRevision,
        RewardTransaction transaction
) {
}
