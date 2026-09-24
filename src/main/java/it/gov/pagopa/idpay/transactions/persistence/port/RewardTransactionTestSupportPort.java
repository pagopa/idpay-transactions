package it.gov.pagopa.idpay.transactions.persistence.port;

import reactor.core.publisher.Flux;

public interface RewardTransactionTestSupportPort {

    Flux<String> deleteByRewardBatchIdAndInitiativeIdReturningIds(String rewardBatchId, String initiativeId);
}
