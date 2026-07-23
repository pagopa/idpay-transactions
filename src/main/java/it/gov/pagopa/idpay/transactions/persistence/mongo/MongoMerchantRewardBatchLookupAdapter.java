package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantRewardBatchLookupPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoMerchantRewardBatchLookupAdapter implements MerchantRewardBatchLookupPort {

    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardBatch> findMerchantBatch(
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
