package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchAtomicMutationPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchAtomicMutationAdapter implements RewardBatchAtomicMutationPort {

    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardBatch> updateTotals(
            String initiativeId,
            String rewardBatchId,
            BatchCountersDTO batchCounters
    ) {
        return rewardBatchRepository.updateTotals(initiativeId, rewardBatchId, batchCounters);
    }
}
