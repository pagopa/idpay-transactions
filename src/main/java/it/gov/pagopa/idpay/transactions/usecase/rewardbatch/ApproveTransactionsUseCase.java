package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.nimbusds.jose.util.Pair;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
@AllArgsConstructor
public class ApproveTransactionsUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;

    public Mono<RewardBatch> execute(String rewardBatchId, TransactionsRequest request, String initiativeId) {
        return rewardBatchRepository.findByIdAndStatus(rewardBatchId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds())
                        .map(trxId -> Pair.of(trxId, batch.getMonth())))
                .flatMap(trxIdAndMonthElaborated -> rewardTransactionRepository.updateStatusAndReturnOld(rewardBatchId, trxIdAndMonthElaborated.getLeft(), RewardBatchTrxStatus.APPROVED, null, trxIdAndMonthElaborated.getRight(), null)
                        .map(trxOld -> Pair.of(trxOld, trxIdAndMonthElaborated.getRight())))
                .reduce(BatchCountersDTO.newBatch(), (acc, trxOld2ActualBatchMonth) -> {
                    RewardTransaction trxOld = trxOld2ActualBatchMonth.getLeft();
                    switch (trxOld.getRewardBatchTrxStatus()) {

                        case RewardBatchTrxStatus.APPROVED ->
                                log.info("Skipping  handler  for transaction  {}:  status  is already  APPROVED", trxOld.getId());

                        case RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.CONSULTABLE ->
                                acc.incrementTrxElaborated();

                        case RewardBatchTrxStatus.SUSPENDED -> {
                            acc.decrementTrxSuspended();
                            if (trxOld.getRewards().get(initiativeId) != null && trxOld.getRewards().get(initiativeId).getAccruedRewardCents() != null) {
                                acc.incrementApprovedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                                acc.decrementSuspendedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                            }
                            RewardBatchSharedUtils.checkAndUpdateTrxElaborated(acc, trxOld2ActualBatchMonth, trxOld);
                        }

                        case RewardBatchTrxStatus.REJECTED -> {
                            acc.decrementTrxRejected();
                            if (trxOld.getRewards().get(initiativeId) != null && trxOld.getRewards().get(initiativeId).getAccruedRewardCents() != null) {
                                acc.incrementApprovedAmountCents(trxOld.getRewards().get(initiativeId).getAccruedRewardCents());
                            }
                        }
                    }
                    return acc;
                })
                .flatMap(acc ->
                        rewardBatchRepository.updateTotals(
                                rewardBatchId,
                                acc)
                );
    }
}

