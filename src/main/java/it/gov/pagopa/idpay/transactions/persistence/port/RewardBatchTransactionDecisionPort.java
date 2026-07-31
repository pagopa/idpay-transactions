package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface RewardBatchTransactionDecisionPort {

    Mono<RewardBatch> prepareEvaluation(String rewardBatchId, String initiativeId);

    Mono<RewardTransaction> updateStatusAndReturnOld(
            String initiativeId,
            String rewardBatchId,
            String transactionId,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason,
            String batchMonth,
            ChecksError checksError
    );
}
