package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import java.util.Set;
import reactor.core.publisher.Mono;

public interface PaymentRestClient {

    Mono<Integer> updateTransactionsStatus(Set<String> transactionIds, SyncTrxStatus status);
}

