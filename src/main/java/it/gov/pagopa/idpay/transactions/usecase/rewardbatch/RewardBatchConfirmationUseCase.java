package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
@Slf4j
@AllArgsConstructor
public class RewardBatchConfirmationUseCase {

    private final RewardBatchRepository rewardBatchRepository;

    public Mono<RewardBatch> execute(String initiativeId, String rewardBatchId) {
        return rewardBatchRepository.findRewardBatchById(rewardBatchId)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId))))
                .filter(rewardBatch -> rewardBatch.getStatus().equals(RewardBatchStatus.EVALUATING)
                        && rewardBatch.getAssigneeLevel().equals(RewardBatchAssignee.L3))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        BAD_REQUEST,
                        REWARD_BATCH_INVALID_REQUEST,
                        ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(rewardBatch -> {
                    Flux<RewardBatch> previousBatchesFlux = rewardBatchRepository.findRewardBatchByMonthBefore(
                            rewardBatch.getMerchantId(),
                            rewardBatch.getPosType(),
                            rewardBatch.getMonth()
                    );
                    Mono<Boolean> hasUnapprovedBatch = previousBatchesFlux
                            .filter(batch -> !batch.getStatus().equals(RewardBatchStatus.APPROVED))
                            .hasElements();
                    return hasUnapprovedBatch
                            .flatMap(isUnapprovedPresent ->
                                    Boolean.TRUE.equals(isUnapprovedPresent)
                                            ? Mono.error(new ClientExceptionWithBody(
                                            BAD_REQUEST,
                                            REWARD_BATCH_INVALID_REQUEST,
                                            ERROR_MESSAGE_PREVIOUS_BATCH_TO_APPROVE.formatted(rewardBatchId)
                                    ))
                                            : Mono.just(rewardBatch)
                            );
                })
                .map(rewardBatch -> {
                    LocalDateTime nowDateTime = LocalDateTime.now();
                    rewardBatch.setStatus(RewardBatchStatus.APPROVING);
                    rewardBatch.setApprovalDate(nowDateTime);
                    rewardBatch.setUpdateDate(nowDateTime);
                    return rewardBatch;
                })
                .flatMap(rewardBatchRepository::save);
    }
}

