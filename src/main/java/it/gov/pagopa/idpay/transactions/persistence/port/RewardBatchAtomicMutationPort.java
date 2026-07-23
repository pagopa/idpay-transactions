package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import reactor.core.publisher.Mono;

public interface RewardBatchAtomicMutationPort {

    Mono<RewardBatch> updateTotals(
            String initiativeId,
            String rewardBatchId,
            BatchCountersDTO batchCounters
    );
}
