package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import reactor.core.publisher.Mono;

public interface RewardBatchFinalApprovalPort {

    Mono<RewardBatch> prepareFinalApproval(String rewardBatchId, String initiativeId);

    Mono<RewardBatch> completeFinalApproval(String rewardBatchId, String initiativeId);
}
