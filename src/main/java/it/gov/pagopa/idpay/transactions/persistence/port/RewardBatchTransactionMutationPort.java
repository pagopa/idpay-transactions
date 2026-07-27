package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface RewardBatchTransactionMutationPort {

    Mono<RewardTransaction> persistInvoiceUpdate(RewardTransaction transaction);

    Mono<RewardTransaction> moveUpdatedInvoice(
            RewardTransaction transaction,
            RewardBatch sourceBatch,
            RewardBatch targetBatch,
            BatchCountersDTO sourceCounters,
            BatchCountersDTO targetCounters
    );

    Mono<Void> reverseInvoice(
            RewardTransaction transaction,
            String initiativeId,
            String sourceBatchId,
            BatchCountersDTO counters
    );

    Mono<Void> reassignSuspendedTransactions(
            RewardBatch sourceBatch,
            RewardBatch targetBatch,
            String initiativeId,
            String sourceMonth
    );

    Mono<RewardTransaction> postponeTransaction(
            RewardTransaction transaction,
            RewardBatch sourceBatch,
            RewardBatch targetBatch,
            BatchCountersDTO sourceCounters,
            BatchCountersDTO targetCounters
    );
}
