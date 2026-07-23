package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchReadPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchReadAdapter implements RewardBatchReadPort {

    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardBatch> findByMerchantIdAndInitiativeIdAndId(
            String merchantId,
            String initiativeId,
            String rewardBatchId
    ) {
        return rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(
                merchantId,
                initiativeId,
                rewardBatchId
        );
    }
}
