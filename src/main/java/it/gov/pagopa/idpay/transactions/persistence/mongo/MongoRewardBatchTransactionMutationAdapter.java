package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionMutationPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchTransactionMutationAdapter implements RewardBatchTransactionMutationPort {

    private final RewardTransactionRepository rewardTransactionRepository;
    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardTransaction> persistInvoiceUpdate(RewardTransaction transaction) {
        return rewardTransactionRepository.save(transaction);
    }

    @Override
    public Mono<RewardTransaction> moveUpdatedInvoice(
            RewardTransaction transaction,
            RewardBatch sourceBatch,
            RewardBatch targetBatch,
            BatchCountersDTO sourceCounters,
            BatchCountersDTO targetCounters
    ) {
        return rewardTransactionRepository.save(transaction)
                .then(rewardBatchRepository.updateTotals(
                        sourceBatch.getInitiativeId(),
                        sourceBatch.getId(),
                        sourceCounters
                ))
                .then(rewardBatchRepository.updateTotals(
                        targetBatch.getInitiativeId(),
                        targetBatch.getId(),
                        targetCounters
                ))
                .thenReturn(transaction);
    }

    @Override
    public Mono<Void> reverseInvoice(
            RewardTransaction transaction,
            String initiativeId,
            String sourceBatchId,
            BatchCountersDTO counters
    ) {
        return rewardTransactionRepository.save(transaction)
                .then(sourceBatchId == null
                        ? Mono.empty()
                        : rewardBatchRepository.updateTotals(initiativeId, sourceBatchId, counters))
                .then();
    }

    @Override
    public Mono<Void> reassignSuspendedTransactions(
            RewardBatch sourceBatch,
            RewardBatch targetBatch,
            String initiativeId,
            String sourceMonth
    ) {
        return rewardTransactionRepository
                .findByFilter(sourceBatch.getId(), initiativeId, List.of(RewardBatchTrxStatus.SUSPENDED))
                .concatMap(transaction -> {
                    transaction.setRewardBatchId(targetBatch.getId());
                    transaction.setStatus(SyncTrxStatus.INVOICED.name());
                    if (transaction.getRewardBatchLastMonthElaborated() == null) {
                        transaction.setRewardBatchLastMonthElaborated(sourceMonth);
                    }
                    return rewardTransactionRepository.save(transaction)
                            .thenReturn(accruedRewardCents(transaction, initiativeId));
                })
                .reduce(new ReassignmentTotals(), ReassignmentTotals::add)
                .flatMap(totals -> rewardBatchRepository.updateTotals(
                        targetBatch.getInitiativeId(),
                        targetBatch.getId(),
                        BatchCountersDTO.newBatch()
                                .incrementInitialAmountCents(totals.accruedRewardCents)
                                .incrementTrxElaborated(totals.transactions)
                                .incrementNumberOfTransactions(totals.transactions)
                                .incrementSuspendedAmountCents(totals.accruedRewardCents)
                                .incrementTrxSuspended(totals.transactions)
                ).then(rewardBatchRepository.updateTotals(
                        sourceBatch.getInitiativeId(),
                        sourceBatch.getId(),
                        BatchCountersDTO.newBatch().decrementNumberOfTransactions(totals.transactions)
                )))
                .then();
    }

    @Override
    public Mono<RewardTransaction> postponeTransaction(
            RewardTransaction transaction,
            RewardBatch sourceBatch,
            RewardBatch targetBatch,
            BatchCountersDTO sourceCounters,
            BatchCountersDTO targetCounters
    ) {
        transaction.setRewardBatchId(targetBatch.getId());
        transaction.setRewardBatchInclusionDate(LocalDateTime.now(ZoneId.systemDefault()));
        transaction.setUpdateDate(LocalDateTime.now(ZoneId.systemDefault()));

        return rewardBatchRepository.updateTotals(
                        sourceBatch.getInitiativeId(),
                        sourceBatch.getId(),
                        sourceCounters
                )
                .then(rewardBatchRepository.updateTotals(
                        targetBatch.getInitiativeId(),
                        targetBatch.getId(),
                        targetCounters
                ))
                .then(rewardTransactionRepository.save(transaction));
    }

    private long accruedRewardCents(RewardTransaction transaction, String initiativeId) {
        if (transaction.getRewards() == null) {
            return 0L;
        }
        Reward reward = transaction.getRewards().get(initiativeId);
        return reward == null || reward.getAccruedRewardCents() == null
                ? 0L
                : reward.getAccruedRewardCents();
    }

    private static final class ReassignmentTotals {
        private long transactions;
        private long accruedRewardCents;

        private ReassignmentTotals add(long rewardCents) {
            transactions++;
            accruedRewardCents += rewardCents;
            return this;
        }
    }
}
