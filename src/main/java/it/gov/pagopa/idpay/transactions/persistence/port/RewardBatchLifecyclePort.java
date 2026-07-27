package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardBatchLifecyclePort {

    Mono<RewardBatch> findBatch(String rewardBatchId);

    Mono<RewardBatch> findBatch(String rewardBatchId, String initiativeId);

    Mono<RewardBatch> findBatchWithStatus(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status
    );

    Flux<RewardBatch> findBatchesWithStatus(RewardBatchStatus status, String initiativeId);

    Flux<RewardBatch> findBatchesWithStatus(
            RewardBatchStatus status,
            String initiativeId,
            Pageable pageable
    );

    Flux<RewardBatch> findDeliverableBatches(
            String initiativeId,
            Pageable pageable
    );

    Flux<RewardBatch> findMerchantBatches(
            String merchantId,
            String initiativeId,
            PosType posType
    );

    Mono<RewardBatch> saveBatch(RewardBatch rewardBatch);

    Mono<RewardBatch> updateEvaluationStatus(
            String rewardBatchId,
            String initiativeId,
            long approvedAmountCents
    );

    Mono<Void> deleteEmptyBatches();
}
