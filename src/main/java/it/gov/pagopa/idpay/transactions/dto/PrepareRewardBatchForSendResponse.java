package it.gov.pagopa.idpay.transactions.dto;

import java.time.LocalDateTime;

public record PrepareRewardBatchForSendResponse(
        String rewardBatchId,
        String previousMonth,
        String referenceMonth,
        LocalDateTime updateDate
) {
}
