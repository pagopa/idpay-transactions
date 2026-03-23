package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.idpay.transactions.dto.batch.TrxSuspendedBatchInfo;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@Slf4j
@AllArgsConstructor
public class EvaluatingRewardBatchesUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;

    public Mono<Long> execute(List<String> rewardBatchesRequest) {
        log.info("[EVALUATING_REWARD_BATCH] Starting evaluation of reward batches with status SENT");
        Flux<RewardBatch> rewardBatchToElaborate;
        if (rewardBatchesRequest == null) {
            rewardBatchToElaborate = rewardBatchRepository.findByStatus(RewardBatchStatus.SENT);
        } else {
            rewardBatchToElaborate = Flux.fromIterable(rewardBatchesRequest)
                    .flatMap(batchId -> rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.SENT));
        }

        return rewardBatchToElaborate
                .flatMap(rewardBatch -> {
                    log.info("[EVALUATING_REWARD_BATCH] Evaluating reward batch {}", Utilities.sanitizeString(rewardBatch.getId()));
                    return rewardTransactionRepository.rewardTransactionsByBatchId(rewardBatch.getId())
                            .thenReturn(rewardBatch)
                            .log("[EVALUATING_REWARD_BATCH]Completed evaluation of transactions for reward batch %s".formatted(Utilities.sanitizeString(rewardBatch.getId())));
                })
                .flatMap(batch -> rewardTransactionRepository.sumSuspendedAccruedRewardCents(batch.getId())
                        .map(suspendedAmountCents -> new TrxSuspendedBatchInfo(batch.getId(), batch.getSuspendedAmountCents(), batch.getInitialAmountCents())))
                .flatMap(suspendedInfo -> rewardBatchRepository.updateStatusAndApprovedAmountCents(suspendedInfo.getRewardBatchId(), RewardBatchStatus.EVALUATING, suspendedInfo.getInitialRewardBatchAmountCents() - suspendedInfo.getSuspendedRewardAmountCents())
                        .log("[EVALUATING_REWARD_BATCH] Reward batch %s moved to status EVALUATING".formatted(Utilities.sanitizeString(suspendedInfo.getRewardBatchId()))))
                .count()
                .doOnSuccess(count ->
                        log.info("[EVALUATING_REWARD_BATCH] Completed evaluation. Total batches processed: {}", count));
    }
}

