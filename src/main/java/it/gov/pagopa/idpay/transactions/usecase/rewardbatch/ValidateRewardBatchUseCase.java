package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import static it.gov.pagopa.idpay.transactions.usecase.rewardbatch.RewardBatchSharedUtils.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.*;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.*;

@Service
@Slf4j
@AllArgsConstructor
public class ValidateRewardBatchUseCase {

    private final RewardBatchRepository rewardBatchRepository;

    public Mono<RewardBatch> execute(String organizationRole, String initiativeId, String rewardBatchId) {
        return rewardBatchRepository.findById(rewardBatchId)
                .switchIfEmpty(Mono.error(new RewardBatchNotFound(
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
                )))
                .flatMap(batch -> {

                    RewardBatchAssignee assignee = batch.getAssigneeLevel();

                    if (assignee == RewardBatchAssignee.L1) {

                        if (!OPERATOR_1.equals(organizationRole)) {
                            return Mono.error(new RoleNotAllowedForL1PromotionException(
                                    ROLE_NOT_ALLOWED_FOR_L1_PROMOTION,
                                    ERROR_MESSAGE_ROLE_NOT_ALLOWED_FOR_L1_PROMOTION
                            ));
                        }

                        long total = batch.getNumberOfTransactions();
                        long elaborated = batch.getNumberOfTransactionsElaborated();

                        if (total == 0 || elaborated < Math.ceil(total * 0.15)) {
                            return Mono.error(new BatchNotElaborated15PercentException(
                                    BATCH_NOT_ELABORATED_15_PERCENT,
                                    ERROR_MESSAGE_BATCH_NOT_ELABORATED_15_PERCENT
                            ));
                        }

                        batch.setAssigneeLevel(RewardBatchAssignee.L2);
                        return rewardBatchRepository.save(batch);
                    }

                    if (assignee == RewardBatchAssignee.L2) {

                        if (!OPERATOR_2.equals(organizationRole)) {
                            return Mono.error(new RoleNotAllowedForL2PromotionException(
                                    ROLE_NOT_ALLOWED_FOR_L2_PROMOTION,
                                    ERROR_MESSAGE_ROLE_NOT_ALLOWED_FOR_L2_PROMOTION
                            ));
                        }

                        batch.setAssigneeLevel(RewardBatchAssignee.L3);
                        return rewardBatchRepository.save(batch);
                    }

                    return Mono.error((new InvalidBatchStateForPromotionException(
                            INVALID_BATCH_STATE_FOR_PROMOTION,
                            ERROR_MESSAGE_INVALID_BATCH_STATE_FOR_PROMOTION
                    )));
                });
    }
}

