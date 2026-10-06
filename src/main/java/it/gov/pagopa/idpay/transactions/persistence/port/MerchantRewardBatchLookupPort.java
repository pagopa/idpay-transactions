package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import reactor.core.publisher.Mono;

public interface MerchantRewardBatchLookupPort {

    Mono<RewardBatch> findMerchantBatch(
            String merchantId,
            String initiativeId,
            String rewardBatchId
    );
}
