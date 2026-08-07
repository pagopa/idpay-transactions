package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchLifecyclePort;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class SqlRewardBatchLifecycleAdapter implements RewardBatchLifecyclePort {

    private final SqlRewardBatchAdapter batchAdapter;
    private final SqlRewardBatchListAdapter batchListAdapter;

    @Override
    public Mono<RewardBatch> findBatch(String rewardBatchId) {
        return batchListAdapter.findBatch(rewardBatchId);
    }

    @Override
    public Mono<RewardBatch> findBatch(String rewardBatchId, String initiativeId) {
        return batchListAdapter.findBatch(rewardBatchId, initiativeId);
    }

    @Override
    public Mono<RewardBatch> findBatchWithStatus(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status
    ) {
        return batchListAdapter.findBatchWithStatus(rewardBatchId, initiativeId, status);
    }

    @Override
    public Flux<RewardBatch> findBatchesWithStatus(RewardBatchStatus status, String initiativeId) {
        return batchListAdapter.findBatchesWithStatus(status, initiativeId);
    }

    @Override
    public Flux<RewardBatch> findBatchesWithStatus(
            RewardBatchStatus status,
            String initiativeId,
            Pageable pageable
    ) {
        return batchListAdapter.findBatchesWithStatus(status, initiativeId, pageable);
    }

    @Override
    public Flux<RewardBatch> findDeliverableBatches(String initiativeId, Pageable pageable) {
        return batchListAdapter.findDeliverableBatches(initiativeId, pageable);
    }

    @Override
    public Flux<RewardBatch> findMerchantBatches(
            String merchantId,
            String initiativeId,
            PosType posType
    ) {
        return batchListAdapter.findMerchantBatches(merchantId, initiativeId, posType);
    }

    @Override
    public Mono<RewardBatch> saveBatch(RewardBatch rewardBatch) {
        return batchAdapter.save(rewardBatch)
                .flatMap(saved -> batchListAdapter.findBatch(saved.getId(), saved.getInitiativeId())
                        .switchIfEmpty(Mono.just(saved)));
    }

    @Override
    public Mono<RewardBatch> updateEvaluationStatus(
            String rewardBatchId,
            String initiativeId,
            long approvedAmountCents
    ) {
        return batchAdapter.updateStatus(rewardBatchId, initiativeId, RewardBatchStatus.EVALUATING)
                .flatMap(saved -> batchListAdapter.findBatch(saved.getId(), saved.getInitiativeId())
                        .switchIfEmpty(Mono.just(saved)));
    }
}
