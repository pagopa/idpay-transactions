package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import reactor.core.publisher.Mono;

public interface RewardBatchAssigneePromotionPort {

    Mono<RewardBatch> findBatchForPromotion(String rewardBatchId, String initiativeId);

    Mono<RewardBatch> promote(
            String rewardBatchId,
            String initiativeId,
            RewardBatchAssignee expectedAssignee,
            RewardBatchAssignee nextAssignee
    );
}
