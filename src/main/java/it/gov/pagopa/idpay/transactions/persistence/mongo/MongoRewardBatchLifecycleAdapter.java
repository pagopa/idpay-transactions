package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchLifecyclePort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchLifecycleAdapter implements RewardBatchLifecyclePort {

    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardBatch> findBatch(String rewardBatchId) {
        return rewardBatchRepository.findById(rewardBatchId);
    }

    @Override
    public Mono<RewardBatch> findBatch(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(rewardBatchId, initiativeId);
    }

    @Override
    public Mono<RewardBatch> findBatchWithStatus(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status
    ) {
        return rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                rewardBatchId,
                initiativeId,
                status
        );
    }

    @Override
    public Flux<RewardBatch> findBatchesWithStatus(RewardBatchStatus status, String initiativeId) {
        return rewardBatchRepository.findByStatusAndInitiativeId(status, initiativeId);
    }

    @Override
    public Flux<RewardBatch> findBatchesWithStatus(
            RewardBatchStatus status,
            String initiativeId,
            Pageable pageable
    ) {
        return rewardBatchRepository.findByStatusAndInitiativeId(status, initiativeId, pageable);
    }

    @Override
    public Flux<RewardBatch> findDeliverableBatches(String initiativeId, Pageable pageable) {
        return rewardBatchRepository.findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                RewardBatchStatus.APPROVED,
                initiativeId,
                0L,
                pageable
        );
    }

    @Override
    public Flux<RewardBatch> findMerchantBatches(
            String merchantId,
            String initiativeId,
            PosType posType
    ) {
        return rewardBatchRepository.findByMerchantIdAndInitiativeIdAndPosType(
                merchantId,
                initiativeId,
                posType
        );
    }

    @Override
    public Mono<RewardBatch> saveBatch(RewardBatch rewardBatch) {
        return rewardBatchRepository.save(rewardBatch);
    }

    @Override
    public Mono<RewardBatch> updateEvaluationStatus(
            String rewardBatchId,
            String initiativeId,
            long approvedAmountCents
    ) {
        return rewardBatchRepository.updateStatusAndApprovedAmountCents(
                rewardBatchId,
                RewardBatchStatus.EVALUATING,
                approvedAmountCents,
                initiativeId
        );
    }
}
