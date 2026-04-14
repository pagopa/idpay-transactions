package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH;

import it.gov.pagopa.common.web.exception.RewardBatchNotFound;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@AllArgsConstructor
public class GetRewardBatchByIdUseCase {

    private final RewardBatchRepository rewardBatchRepository;

    public Mono<RewardBatch> execute(String merchantId, String initiativeId, String rewardBatchId) {
        return rewardBatchRepository.findByMerchantIdAndInitiativeIdAndId(merchantId, initiativeId, rewardBatchId)
                .switchIfEmpty(Mono.error(new RewardBatchNotFound(
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
                )));
    }
}

