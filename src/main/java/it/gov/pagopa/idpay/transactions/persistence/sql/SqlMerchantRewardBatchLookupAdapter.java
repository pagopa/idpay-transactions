package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantRewardBatchLookupPort;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class SqlMerchantRewardBatchLookupAdapter implements MerchantRewardBatchLookupPort {

    private final SqlRewardBatchListAdapter batchListAdapter;

    @Override
    public Mono<RewardBatch> findMerchantBatch(
            String merchantId,
            String initiativeId,
            String rewardBatchId
    ) {
        return batchListAdapter.findMerchantBatch(merchantId, initiativeId, rewardBatchId);
    }
}
