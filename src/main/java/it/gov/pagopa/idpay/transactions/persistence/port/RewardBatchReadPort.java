package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import reactor.core.publisher.Mono;

public interface RewardBatchReadPort {

    Mono<RewardBatch> findByMerchantIdAndInitiativeIdAndId(
            String merchantId,
            String initiativeId,
            String rewardBatchId
    );
}
