package it.gov.pagopa.idpay.transactions.persistence.port;

import reactor.core.publisher.Mono;

public interface InitiativeTransactionDeletionPort {

    Mono<Long> deleteTransactions(String initiativeId);
}
