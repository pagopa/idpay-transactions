package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchListPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchListAdapter implements RewardBatchListPort {

    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Flux<RewardBatch> findRewardBatches(
            String merchantId,
            String initiativeId,
            String status,
            String assigneeLevel,
            String month,
            boolean isOperator,
            Pageable pageable
    ) {
        return rewardBatchRepository.findRewardBatchesCombined(
                merchantId,
                initiativeId,
                status,
                assigneeLevel,
                month,
                isOperator,
                pageable
        );
    }

    @Override
    public Mono<Long> countRewardBatches(
            String merchantId,
            String initiativeId,
            String status,
            String assigneeLevel,
            String month,
            boolean isOperator
    ) {
        return rewardBatchRepository.getCountCombined(
                merchantId,
                initiativeId,
                status,
                assigneeLevel,
                month,
                isOperator
        );
    }

    @Override
    public Flux<RewardBatch> findBatchesBeforeMonth(
            String merchantId,
            String initiativeId,
            PosType posType,
            String month
    ) {
        return rewardBatchRepository.findRewardBatchByMonthBefore(
                merchantId,
                initiativeId,
                posType,
                month
        );
    }
}
