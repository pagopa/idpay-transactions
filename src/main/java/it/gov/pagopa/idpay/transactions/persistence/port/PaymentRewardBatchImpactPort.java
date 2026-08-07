package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.PaymentBatchEligibility;
import it.gov.pagopa.idpay.transactions.model.PaymentRewardBatchImpact;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface PaymentRewardBatchImpactPort {

    Mono<PaymentBatchEligibility> findEligibility(String merchantId, String transactionId);

    Mono<RewardTransaction> applyImpact(PaymentRewardBatchImpact impact);
}
