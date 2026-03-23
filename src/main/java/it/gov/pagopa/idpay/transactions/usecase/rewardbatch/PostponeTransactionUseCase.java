package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;

@Service
@Slf4j
@AllArgsConstructor
public class PostponeTransactionUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final RewardTransactionRepository rewardTransactionRepository;
    private final RewardBatchService rewardBatchService;

    public Mono<Void> execute(String merchantId, String initiativeId, String rewardBatchId, String transactionId, LocalDate initiativeEndDate) {

        return rewardTransactionRepository.findTransactionInBatch(merchantId, rewardBatchId, transactionId)
                .switchIfEmpty(Mono.error(new ClientExceptionNoBody(
                        HttpStatus.NOT_FOUND,
                        String.format(ExceptionMessage.TRANSACTION_NOT_FOUND, transactionId)
                )))
                .flatMap(trx -> {

                    long accruedRewardCents = trx.getRewards().get(initiativeId).getAccruedRewardCents();

                    return rewardBatchRepository.findById(rewardBatchId)
                            .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                                    HttpStatus.NOT_FOUND,
                                    ExceptionCode.REWARD_BATCH_NOT_FOUND,
                                    String.format(ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH, rewardBatchId)
                            )))
                            .flatMap(currentBatch -> {

                                if (currentBatch.getStatus() != RewardBatchStatus.CREATED) {
                                    return Mono.error(new ClientExceptionWithBody(
                                            HttpStatus.BAD_REQUEST,
                                            ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                                            ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH
                                    ));
                                }

                                YearMonth currentBatchMonth = YearMonth.parse(currentBatch.getMonth());
                                YearMonth nextBatchMonth = currentBatchMonth.plusMonths(1);

                                YearMonth maxAllowedMonth = YearMonth.from(initiativeEndDate).plusMonths(1);

                                if (nextBatchMonth.isAfter(maxAllowedMonth)) {
                                    return Mono.error(new ClientExceptionWithBody(
                                            HttpStatus.BAD_REQUEST,
                                            ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED,
                                            ExceptionMessage.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED
                                    ));
                                }

                                return rewardBatchService.findOrCreateBatch(
                                                currentBatch.getMerchantId(),
                                                currentBatch.getPosType(),
                                                nextBatchMonth.toString(),
                                                currentBatch.getBusinessName()
                                        )
                                        .flatMap(nextBatch -> {

                                            if (nextBatch.getStatus() != RewardBatchStatus.CREATED) {
                                                return Mono.error(new ClientExceptionNoBody(
                                                        HttpStatus.BAD_REQUEST,
                                                        ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH
                                                ));
                                            }

                                            boolean isTrxSuspended = RewardBatchTrxStatus.SUSPENDED.equals(trx.getRewardBatchTrxStatus());
                                            BatchCountersDTO oldBatchCounters = BatchCountersDTO.newBatch()
                                                    .decrementInitialAmountCents(accruedRewardCents)
                                                    .decrementNumberOfTransactions();
                                            BatchCountersDTO newBatchCounters = BatchCountersDTO.newBatch()
                                                    .incrementInitialAmountCents(accruedRewardCents)
                                                    .incrementNumberOfTransactions(1L);
                                            if (isTrxSuspended) {

                                                oldBatchCounters
                                                        .decrementSuspendedAmountCents(accruedRewardCents)
                                                        .decrementTrxElaborated()
                                                        .decrementTrxSuspended();

                                                newBatchCounters
                                                        .incrementSuspendedAmountCents(accruedRewardCents)
                                                        .incrementTrxElaborated()
                                                        .incrementTrxSuspended();
                                            }

                                            return rewardBatchRepository.updateTotals(currentBatch.getId(), oldBatchCounters)
                                                    .then(rewardBatchRepository.updateTotals(nextBatch.getId(), newBatchCounters))
                                                    .then(Mono.defer(() -> {

                                                        trx.setRewardBatchId(nextBatch.getId());
                                                        trx.setRewardBatchInclusionDate(LocalDateTime.now());
                                                        trx.setUpdateDate(LocalDateTime.now());

                                                        return rewardTransactionRepository.save(trx);
                                                    }));
                                        });
                            });
                })
                .then();
    }
}

