package it.gov.pagopa.idpay.transactions.model;

import java.time.LocalDateTime;

public record PreparedRewardBatch(
        String rewardBatchId,
        String previousMonth,
        String referenceMonth,
        LocalDateTime updateDate
) {
}
