package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import reactor.core.publisher.Mono;

public interface RewardBatchWritePort {

    Mono<RewardBatch> save(RewardBatch rewardBatch);
}
