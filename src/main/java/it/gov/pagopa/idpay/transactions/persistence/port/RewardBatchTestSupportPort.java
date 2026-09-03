package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.PreparedRewardBatch;
import reactor.core.publisher.Mono;

public interface RewardBatchTestSupportPort {

    Mono<PreparedRewardBatch> prepareForSend(
            String initiativeId,
            String rewardBatchId,
            int searchHorizonMonths
    );
}
