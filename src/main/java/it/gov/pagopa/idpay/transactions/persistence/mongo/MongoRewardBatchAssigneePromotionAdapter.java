package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.BATCH_NOT_ELABORATED_15_PERCENT;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_BATCH_NOT_ELABORATED_15_PERCENT;

import it.gov.pagopa.common.web.exception.BatchNotElaborated15PercentException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchAssigneePromotionPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchAssigneePromotionAdapter implements RewardBatchAssigneePromotionPort {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;

    @Override
    public Mono<RewardBatch> findBatchForPromotion(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(rewardBatchId, initiativeId);
    }

    @Override
    public Mono<RewardBatch> promote(
            String rewardBatchId,
            String initiativeId,
            RewardBatchAssignee expectedAssignee,
            RewardBatchAssignee nextAssignee
    ) {
        validateTransition(expectedAssignee, nextAssignee);
        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(rewardBatchId, initiativeId)
                .filter(batch -> batch.getAssigneeLevel() == expectedAssignee)
                .flatMap(batch -> {
                    Mono<Void> eligibility = requiresElaborationCheck(expectedAssignee)
                            ? verifyElaborationThreshold(rewardBatchId, initiativeId)
                            : Mono.empty();
                    return eligibility.then(Mono.defer(() -> {
                        batch.setAssigneeLevel(nextAssignee);
                        batch.setUpdateDate(LocalDateTime.now(ZONEID));
                        return rewardBatchRepository.save(batch);
                    }));
                });
    }

    private static void validateTransition(
            RewardBatchAssignee expectedAssignee,
            RewardBatchAssignee nextAssignee
    ) {
        boolean supported = (expectedAssignee == RewardBatchAssignee.L1
                && nextAssignee == RewardBatchAssignee.L2)
                || (expectedAssignee == RewardBatchAssignee.L2
                && nextAssignee == RewardBatchAssignee.L3);
        if (!supported) {
            throw new IllegalArgumentException("Unsupported reward batch assignee promotion");
        }
    }

    private Mono<Void> verifyElaborationThreshold(String rewardBatchId, String initiativeId) {
        return rewardTransactionRepository.findByRewardBatchIdAndInitiativeId(rewardBatchId, initiativeId)
                .reduce(ElaborationCounts.empty(), ElaborationCounts::include)
                .flatMap(counts -> counts.total() > 0 && counts.elaborated() < Math.ceil(counts.total() * 0.15)
                        ? Mono.error(new BatchNotElaborated15PercentException(
                        BATCH_NOT_ELABORATED_15_PERCENT,
                        ERROR_MESSAGE_BATCH_NOT_ELABORATED_15_PERCENT
                ))
                        : Mono.empty());
    }

    private static boolean requiresElaborationCheck(RewardBatchAssignee expectedAssignee) {
        return expectedAssignee == RewardBatchAssignee.L1;
    }

    private record ElaborationCounts(long total, long elaborated) {

        private static ElaborationCounts empty() {
            return new ElaborationCounts(0L, 0L);
        }

        private ElaborationCounts include(RewardTransaction transaction) {
            boolean isElaborated = transaction.getRewardBatchTrxStatus() == RewardBatchTrxStatus.SUSPENDED
                    || transaction.getRewardBatchTrxStatus() == RewardBatchTrxStatus.APPROVED
                    || transaction.getRewardBatchTrxStatus() == RewardBatchTrxStatus.REJECTED;
            return new ElaborationCounts(total + 1L, elaborated + (isElaborated ? 1L : 0L));
        }
    }
}
