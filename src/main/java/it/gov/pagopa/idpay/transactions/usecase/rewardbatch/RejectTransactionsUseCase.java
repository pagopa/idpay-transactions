package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.nimbusds.jose.util.Pair;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
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
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
@AllArgsConstructor
public class RejectTransactionsUseCase {

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
                .flatMapMany(batch -> Flux.fromIterable(request.getTransactionIds())
                        .map(trxId -> Pair.of(trxId, batch.getMonth())))
                .flatMap(trxId2ActualBatchMont -> rewardTransactionRepository
                        .updateStatusAndReturnOld(rewardBatchId, trxId2ActualBatchMont.getLeft(), RewardBatchTrxStatus.REJECTED, reason, trxId2ActualBatchMont.getRight(), checksErrorModel)
                        .map(trxOld -> {
                            if (trxOld != null) {
                                log.info(
                                        "[REJECT_TRANSACTION] Transaction {} rejected. batchId: {}, initiativeId: {}",
                                        trxOld.getId(),
                                        Utilities.sanitizeString(rewardBatchId),
                                        Utilities.sanitizeString(initiativeId)
                                );
                            }
                            return Pair.of(trxOld, trxId2ActualBatchMont.getRight());
                        })
                )
                .reduce(BatchCountersDTO.newBatch(),
                        (acc, trxOld2ActualRewardBatchMonth) -> {

                            RewardTransaction trxOld = trxOld2ActualRewardBatchMonth.getLeft();

                            if (trxOld == null) {
                                return acc;
                            }

                            Long accrued = trxOld.getRewards().get(initiativeId) != null
                                    ? trxOld.getRewards().get(initiativeId).getAccruedRewardCents()
                                    : null;

                            switch (trxOld.getRewardBatchTrxStatus()) {

                                case RewardBatchTrxStatus.REJECTED ->
                                        log.info("Skipping  handler  for transaction  {}:  status  is already  REJECTED", trxOld.getId());

                                case RewardBatchTrxStatus.APPROVED -> {
                                    acc.incrementTrxRejected();

                                    if (accrued != null) {
                                        acc.decrementApprovedAmountCents(accrued);
                                    }
                                }

                                case RewardBatchTrxStatus.TO_CHECK,
                                     RewardBatchTrxStatus.CONSULTABLE -> {
                                    acc.incrementTrxElaborated();
                                    acc.incrementTrxRejected();

                                    if (accrued != null) {
                                        acc.decrementApprovedAmountCents(accrued);
                                    }
                                }

                                case RewardBatchTrxStatus.SUSPENDED -> {
                                    acc.decrementTrxSuspended();
                                    acc.incrementTrxRejected();
                                    if (accrued != null) {
                                        acc.decrementSuspendedAmountCents(accrued);
                                    }

                                    RewardBatchSharedUtils.checkAndUpdateTrxElaborated(acc, trxOld2ActualRewardBatchMonth, trxOld);
                                }

                            }

                            return acc;
                        })
                .flatMap(acc -> {

                    auditUtilities.logTransactionsStatusChanged(
                            RewardBatchTrxStatus.REJECTED.name(),
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

