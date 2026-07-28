package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface InvoicedTransactionAssignmentPort {

    Flux<RewardTransaction> findInvoicedTransactionsWithoutBatch(int batchSize);

    Mono<RewardTransaction> findInvoicedTransactionWithoutBatch(String transactionId);

    Mono<RewardTransaction> assignInvoicedTransaction(
            RewardTransaction transaction,
            RewardBatch batch,
            int samplingKey
    );
}
