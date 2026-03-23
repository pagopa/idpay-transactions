package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.YearMonth;

@Service
@Slf4j
@AllArgsConstructor
public class SendRewardBatchUseCase {

    private final RewardBatchRepository rewardBatchRepository;

    public Mono<Void> execute(String merchantId, String batchId) {
        return rewardBatchRepository.findById(batchId)
                .switchIfEmpty(Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
                        ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND)))
                .flatMap(batch -> {
                    if (!merchantId.equals(batch.getMerchantId())) {
                        log.warn("[SEND_REWARD_BATCHES] Merchant id mismatch !");
                        return Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND,
                                ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND));
                    }
                    if (batch.getStatus() != RewardBatchStatus.CREATED) {
                        return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                                ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST));
                    }
                    YearMonth batchMonth = YearMonth.parse(batch.getMonth());
                    if (!YearMonth.now().isAfter(batchMonth)) {
                        log.warn("[SEND_REWARD_BATCHES] Batch month too early to be sent !");
                        return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                                ExceptionConstants.ExceptionCode.REWARD_BATCH_MONTH_TOO_EARLY));
                    }

                    return noPreviousBatchesInCreatedStatus(merchantId, batchMonth, batch.getPosType())
                            .flatMap(allPreviousSent -> {
                                if (Boolean.FALSE.equals(allPreviousSent)) {
                                    log.warn("[SEND_REWARD_BATCHES] Previous batches of type {} not sent yet for merchant {}!",
                                            batch.getPosType(), Utilities.sanitizeString(merchantId));
                                    return Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST,
                                            ExceptionConstants.ExceptionCode.REWARD_BATCH_PREVIOUS_NOT_SENT));
                                }

                                LocalDateTime dateTimeNow = LocalDateTime.now();
                                batch.setStatus(RewardBatchStatus.SENT);
                                batch.setMerchantSendDate(dateTimeNow);
                                batch.setUpdateDate(dateTimeNow);
                                return rewardBatchRepository.save(batch);
                            })
                            .then();
                });
    }

    private Mono<Boolean> noPreviousBatchesInCreatedStatus(String merchantId, YearMonth currentMonth, PosType posType) {
        return rewardBatchRepository.findByMerchantIdAndPosType(merchantId, posType)
                .filter(batch -> {
                    YearMonth batchMonth = YearMonth.parse(batch.getMonth());
                    return batchMonth.isBefore(currentMonth);
                })
                .filter(batch -> batch.getStatus() == RewardBatchStatus.CREATED)
                .hasElements()
                .map(hasCreated -> !hasCreated);
    }
}

