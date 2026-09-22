package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.TransactionProjectionDTO;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import java.util.List;
import java.util.Set;
import reactor.core.publisher.Mono;

public interface PaymentRestClient {

    Mono<Integer> updateTransactionsStatus(Set<String> transactionIds, SyncTrxStatus status);

    Mono<List<TransactionProjectionDTO>> getTransactionsProjectionByIds(Set<String> transactionIds);
}

