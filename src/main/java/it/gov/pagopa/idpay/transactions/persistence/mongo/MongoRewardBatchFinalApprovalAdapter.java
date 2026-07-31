package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchFinalApprovalPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchFinalApprovalAdapter implements RewardBatchFinalApprovalPort {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;

    @Override
    public Mono<RewardBatch> prepareFinalApproval(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                        rewardBatchId,
                        initiativeId,
                        RewardBatchStatus.APPROVING
                )
                .filter(batch -> batch.getAssigneeLevel() == RewardBatchAssignee.L3)
                .flatMap(batch -> rewardTransactionRepository.findByFilter(
                                rewardBatchId,
                                initiativeId,
                                List.of(RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.CONSULTABLE)
                        )
                        .concatMap(transaction -> {
                            transaction.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
                            return rewardTransactionRepository.save(transaction);
                        })
                        .then(Mono.just(batch)));
    }

    @Override
    public Mono<RewardBatch> completeFinalApproval(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(rewardBatchId, initiativeId)
                .flatMap(batch -> {
                    if (batch.getStatus() == RewardBatchStatus.APPROVED) {
                        return Mono.just(batch);
                    }
                    if (batch.getStatus() != RewardBatchStatus.APPROVING
                            || batch.getAssigneeLevel() != RewardBatchAssignee.L3) {
                        return Mono.empty();
                    }

                    batch.setStatus(RewardBatchStatus.APPROVED);
                    batch.setUpdateDate(LocalDateTime.now(ZONEID));
                    return rewardBatchRepository.save(batch);
                });
    }
}
