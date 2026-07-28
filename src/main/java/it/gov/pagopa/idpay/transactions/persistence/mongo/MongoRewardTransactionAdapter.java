package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoicedTransactionAssignmentPort;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoiceTransactionLookupPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionDecisionPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionReadPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSynchronizationPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH;

@Component
@RequiredArgsConstructor
public class MongoRewardTransactionAdapter implements
        RewardTransactionSynchronizationPort,
        InvoicedTransactionAssignmentPort,
        RewardBatchTransactionReadPort,
        RewardBatchTransactionDecisionPort,
        InvoiceTransactionLookupPort {

    private final RewardTransactionRepository rewardTransactionRepository;
    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardTransaction> upsert(RewardTransaction transaction) {
        return rewardTransactionRepository.save(transaction);
    }

    @Override
    public Flux<RewardTransaction> findInvoicedTransactionsWithoutBatch(int batchSize) {
        return rewardTransactionRepository.findInvoicedTransactionsWithoutBatch(batchSize);
    }

    @Override
    public Mono<RewardTransaction> findInvoicedTransactionWithoutBatch(String transactionId) {
        return rewardTransactionRepository.findById(transactionId)
                .flatMap(transaction -> rewardTransactionRepository.findInvoicedTrxByIdWithoutBatch(
                        transaction.getInitiatives().getFirst(),
                        transaction.getMerchantId(),
                        transactionId
                ));
    }

    @Override
    public Mono<RewardTransaction> assignInvoicedTransaction(
            RewardTransaction transaction,
            RewardBatch batch,
            int samplingKey
    ) {
        String initiativeId = initiativeId(transaction);
        if (!initiativeId.equals(batch.getInitiativeId())) {
            return Mono.error(new IllegalArgumentException(
                    "Transaction and reward batch must belong to the same initiative"
            ));
        }

        return findOrCreateBatch(batch)
                .flatMap(rewardBatch -> {
                    if (rewardBatch.getStatus() != RewardBatchStatus.CREATED) {
                        return Mono.error(new it.gov.pagopa.common.web.exception.ClientExceptionNoBody(
                                HttpStatus.BAD_REQUEST,
                                REWARD_BATCH_STATUS_MISMATCH
                        ));
                    }

                    BatchCountersDTO counters = BatchCountersDTO.newBatch()
                            .incrementInitialAmountCents(accruedRewardCents(transaction, initiativeId))
                            .incrementNumberOfTransactions(1L);

                    return rewardBatchRepository.updateTotals(
                                    rewardBatch.getInitiativeId(),
                                    rewardBatch.getId(),
                                    counters
                            )
                            .flatMap(updatedBatch -> {
                                transaction.setRewardBatchId(updatedBatch.getId());
                                transaction.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
                                transaction.setRewardBatchInclusionDate(LocalDateTime.now());
                                transaction.setRewardBatchRejectionReason(null);
                                transaction.setSamplingKey(samplingKey);
                                transaction.setUpdateDate(LocalDateTime.now());
                                return rewardTransactionRepository.save(transaction);
                            });
                });
    }

    @Override
    public Flux<RewardTransaction> findBatchTransactions(
            String rewardBatchId,
            String initiativeId,
            List<RewardBatchTrxStatus> statuses
    ) {
        return rewardTransactionRepository.findByFilter(rewardBatchId, initiativeId, statuses);
    }

    @Override
    public Mono<RewardTransaction> findTransactionInBatch(
            String initiativeId,
            String merchantId,
            String rewardBatchId,
            String transactionId
    ) {
        return rewardTransactionRepository.findTransactionInBatch(
                initiativeId,
                merchantId,
                rewardBatchId,
                transactionId
        );
    }

    @Override
    public Mono<RewardTransaction> findInvoiceTransaction(String merchantId, String transactionId) {
        return rewardTransactionRepository.findTransaction(merchantId, transactionId);
    }

    @Override
    public Mono<RewardTransaction> updateStatusAndReturnOld(
            String initiativeId,
            String rewardBatchId,
            String transactionId,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason,
            String batchMonth,
            ChecksError checksError
    ) {
        return rewardTransactionRepository.updateStatusAndReturnOld(
                initiativeId,
                rewardBatchId,
                transactionId,
                newStatus,
                reason,
                batchMonth,
                checksError
        );
    }

    private Mono<RewardBatch> findOrCreateBatch(RewardBatch batch) {
        return rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                        batch.getInitiativeId(),
                        batch.getMerchantId(),
                        batch.getPosType(),
                        batch.getMonth()
                )
                .switchIfEmpty(Mono.defer(() -> rewardBatchRepository.save(batch)
                        .onErrorResume(DuplicateKeyException.class, exception ->
                                rewardBatchRepository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                                        batch.getInitiativeId(),
                                        batch.getMerchantId(),
                                        batch.getPosType(),
                                        batch.getMonth()
                                ))));
    }

    private long accruedRewardCents(RewardTransaction transaction, String initiativeId) {
        return Optional.ofNullable(transaction.getRewards())
                .map(rewards -> rewards.get(initiativeId))
                .map(Reward::getAccruedRewardCents)
                .orElse(0L);
    }

    private String initiativeId(RewardTransaction transaction) {
        List<String> initiatives = transaction.getInitiatives();
        if (initiatives == null || initiatives.size() != 1 || initiatives.getFirst().isBlank()) {
            throw new IllegalArgumentException("A reward transaction must have exactly one initiative");
        }
        return initiatives.getFirst();
    }
}
