package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.nimbusds.jose.util.Pair;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
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
public class SuspendTransactionsUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;
    private final ChecksErrorMapper checksErrorMapper;
    private final AuditUtilities auditUtilities;

    public Mono<RewardBatch> execute(String rewardBatchId, String initiativeId, TransactionsRequest request) {

        RewardBatchSharedUtils.validChecksError(request.getChecksError());

        ChecksError checksErrorModel = checksErrorMapper.toModel(request.getChecksError());
        ReasonDTO reason = RewardBatchSharedUtils.generateReasonDto(request);

        return rewardBatchRepository.findByIdAndStatus(rewardBatchId, RewardBatchStatus.EVALUATING)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND_OR_INVALID_STATE,
                        ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_OR_INVALID_STATE_BATCH.formatted(rewardBatchId))))
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds()).map(trxId -> Pair.of(trxId, batch.getMonth())))
                .flatMap(trxId2ActualBatchMonth -> rewardTransactionRepository
                        .updateStatusAndReturnOld(rewardBatchId, trxId2ActualBatchMonth.getLeft(), RewardBatchTrxStatus.SUSPENDED, reason, trxId2ActualBatchMonth.getRight(), checksErrorModel)
                        .map(trxOld -> Pair.of(trxOld, trxId2ActualBatchMonth.getRight()))
                )
                .reduce(BatchCountersDTO.newBatch(), (acc, trxOld2ActualRewardBatch) -> {

                    RewardTransaction trxOld = trxOld2ActualRewardBatch.getLeft();

                    if (trxOld == null) {
                        return acc;
                    }

                    Long accrued = trxOld.getRewards().get(initiativeId) != null
                            ? trxOld.getRewards().get(initiativeId).getAccruedRewardCents()
                            : null;

                    switch (trxOld.getRewardBatchTrxStatus()) {

                        case RewardBatchTrxStatus.SUSPENDED ->
                                RewardBatchSharedUtils.suspendedTransactionAlreadySuspended(acc, trxOld2ActualRewardBatch, trxOld);

                        case RewardBatchTrxStatus.APPROVED -> {
                            acc.incrementTrxSuspended();

                            if (accrued != null) {
                                acc.decrementApprovedAmountCents(accrued);
                                acc.incrementSuspendedAmountCents(accrued);
                            }
                        }

                        case RewardBatchTrxStatus.TO_CHECK,
                             RewardBatchTrxStatus.CONSULTABLE -> {
                            acc.incrementTrxElaborated();
                            acc.incrementTrxSuspended();

                            if (accrued != null) {
                                acc.decrementApprovedAmountCents(accrued);
                                acc.incrementSuspendedAmountCents(accrued);
                            }
                        }

                        case RewardBatchTrxStatus.REJECTED -> {
                            acc.decrementTrxRejected();
                            acc.incrementTrxSuspended();
                            if (accrued != null) {
                                acc.incrementSuspendedAmountCents(accrued);
                            }
                        }
                    }

                    return acc;
                })
                .flatMap(acc -> {

                    auditUtilities.logTransactionsStatusChanged(
                            RewardBatchTrxStatus.SUSPENDED.name(),
                            initiativeId,
                            request.getTransactionIds().toString(),
                            request.getChecksError()
                    );

                    return rewardBatchRepository.updateTotals(
                            rewardBatchId,
                            acc
                    );
                });
    }
}

