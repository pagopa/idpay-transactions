package it.gov.pagopa.idpay.transactions.connector.rest;

import reactor.core.publisher.Mono;

public interface PaymentRestClient {

    Mono<Void> cancelTransaction(String transactionId, String merchantId, String acquirerId, String pointOfSaleId);

}

