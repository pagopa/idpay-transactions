package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchWritePort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchWriteAdapter implements RewardBatchWritePort {

    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardBatch> save(RewardBatch rewardBatch) {
        return rewardBatchRepository.save(rewardBatch);
    }
}
