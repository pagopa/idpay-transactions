package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;

public record PaymentBatchEligibility(
        String transactionId,
        String initiativeId,
        String merchantId,
        String rewardBatchId,
        String transactionStatus,
        RewardBatchStatus batchStatus,
        RewardBatchTrxStatus batchTransactionStatus
) {
}
