package it.gov.pagopa.idpay.transactions.persistence.port;

import reactor.core.publisher.Mono;

public interface SuspendedTransactionReassignmentPort {

    Mono<Void> reassignSuspendedTransactions(String sourceBatchId, String initiativeId);
}
