package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.TransactionNotes;
import reactor.core.publisher.Mono;

public interface RewardBatchTransactionDecisionPort {

    Mono<RewardBatch> prepareEvaluation(String rewardBatchId, String initiativeId);

    Mono<RewardTransaction> updateStatusAndReturnOld(
            String initiativeId,
            String rewardBatchId,
            String transactionId,
            RewardBatchTrxStatus newStatus,
            TransactionNotes notes,
            String batchMonth,
            ChecksError checksError
    );

    /**
     * Compatibility overload for clients using the previous single-note contract.
     * The legacy reason is used as both the internal and official note so that
     * state transitions remain valid while clients migrate to the new contract.
     *
     * @deprecated use {@link #updateStatusAndReturnOld(String, String, String,
     * RewardBatchTrxStatus, TransactionNotes, String, ChecksError)} instead
     */
    @SuppressWarnings("java:S1133")
    @Deprecated(since = "1.2.3", forRemoval = false)
    default Mono<RewardTransaction> updateStatusAndReturnOld(
            String initiativeId,
            String rewardBatchId,
            String transactionId,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason,
            String batchMonth,
            ChecksError checksError
    ) {
        return updateStatusAndReturnOld(
                initiativeId,
                rewardBatchId,
                transactionId,
                newStatus,
                new TransactionNotes(reason, reason),
                batchMonth,
                checksError
        );
    }
}
