package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
@AllArgsConstructor
public class RewardBatchConfirmationBatchUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;
    private final RewardBatchService rewardBatchService;
    private final GenerateAndSaveCsvUseCase generateAndSaveCsvUseCase;

    public Mono<Void> execute(String initiativeId, List<String> rewardBatchIds) {
        return RewardBatchSharedUtils.processBatchesOrchestrator(
                rewardBatchRepository, initiativeId, rewardBatchIds,
                RewardBatchStatus.APPROVING, this::processSingleBatchConfirmation);
    }

    public Mono<RewardBatch> processSingleBatchConfirmation(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))

                .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.APPROVING)
                        && rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3))

                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(originalBatch -> {
                    Mono<Void> transactionsUpdate = updateAndSaveRewardTransactionsToApprove(rewardBatchId, initiativeId);
                    return transactionsUpdate.thenReturn(originalBatch);
                })
                .flatMap(batch -> handleSuspendedTransactions(batch, initiativeId))
                .flatMap(originalBatch -> {
                    originalBatch.setStatus(RewardBatchStatus.APPROVED);
                    originalBatch.setUpdateDate(LocalDateTime.now());
                    return rewardBatchRepository.save(originalBatch);
                })
                .flatMap(savedBatch ->
                        generateAndSaveCsvUseCase.execute(rewardBatchId, initiativeId, savedBatch.getMerchantId())
                                .onErrorResume(e -> {
                                    log.error("Critical error while generating CSV for batch {}", Utilities.sanitizeString(rewardBatchId), e);
                                    return Mono.just("ERROR");
                                })
                                .thenReturn(savedBatch)
                );
    }

    Mono<RewardBatch> handleSuspendedTransactions(RewardBatch originalBatch, String initiativeId) {
        if (originalBatch.getNumberOfTransactionsSuspended() == null || originalBatch.getNumberOfTransactionsSuspended() <= 0) {
            log.info("numberOfTransactionSuspended = 0 for batch {}", originalBatch.getId());
            return Mono.just(originalBatch);
        }

        long countToMove = originalBatch.getNumberOfTransactionsSuspended();

        return rewardBatchService.findOrCreateBatch(originalBatch.getMerchantId(),
                        originalBatch.getPosType(),
                        RewardBatchSharedUtils.addOneMonth(originalBatch.getMonth()),
                        originalBatch.getBusinessName())
                .flatMap(newBatch -> updateAndSaveRewardTransactionsSuspended(originalBatch.getId(), initiativeId, newBatch.getId(), originalBatch.getMonth())
                        .flatMap(totalAccrued -> {
                            BatchCountersDTO batchCounters = BatchCountersDTO.newBatch()
                                    .incrementInitialAmountCents(totalAccrued)
                                    .incrementNumberOfTransactions(countToMove)
                                    .incrementSuspendedAmountCents(totalAccrued)
                                    .incrementTrxSuspended(countToMove);
                            return rewardBatchRepository.updateTotals(newBatch.getId(), batchCounters);
                        }))
                .thenReturn(originalBatch);
    }

    public Mono<Void> updateAndSaveRewardTransactionsToApprove(String oldBatchId, String initiativeId) {
        List<RewardBatchTrxStatus> statusList = new ArrayList<>();
        statusList.add(RewardBatchTrxStatus.TO_CHECK);
        statusList.add(RewardBatchTrxStatus.CONSULTABLE);

        return rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, statusList)
                .collectList()
                .doOnNext(list ->
                        log.info("Found {} transactions to approve for batch {}",
                                list.size(),
                                Utilities.sanitizeString(oldBatchId))
                )
                .flatMapMany(Flux::fromIterable)
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
                    return rewardTransactionRepository.save(rewardTransaction);
                })
                .then();
    }

    public Mono<Long> updateAndSaveRewardTransactionsSuspended(String oldBatchId, String initiativeId, String newBatchId, String oldMonth) {
        List<RewardBatchTrxStatus> statusList = List.of(RewardBatchTrxStatus.SUSPENDED);

        return rewardTransactionRepository.findByFilter(oldBatchId, initiativeId, statusList)
                .switchIfEmpty(Flux.defer(() -> {
                    log.info("No suspended transactions found for the batch {}", Utilities.sanitizeString(oldBatchId));
                    return Flux.empty();
                }))
                .flatMap(rewardTransaction -> {
                    rewardTransaction.setRewardBatchId(newBatchId);
                    if (rewardTransaction.getRewardBatchLastMonthElaborated() == null) {
                        rewardTransaction.setRewardBatchLastMonthElaborated(oldMonth);
                    }

                    Long rewardCents = 0L;
                    if (rewardTransaction.getRewards() != null &&
                            rewardTransaction.getRewards().get(initiativeId) != null) {
                        rewardCents = rewardTransaction.getRewards().get(initiativeId).getAccruedRewardCents();
                    }

                    return rewardTransactionRepository.save(rewardTransaction)
                            .thenReturn(rewardCents != null ? rewardCents : 0L);
                })
                .reduce(0L, Long::sum)
                .doOnNext(total -> log.info("Total suspended reward cents from old batch {}: {}", newBatchId, total));
    }
}

