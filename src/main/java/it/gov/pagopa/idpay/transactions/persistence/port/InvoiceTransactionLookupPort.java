package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface InvoiceTransactionLookupPort {

    Mono<RewardTransaction> findInvoiceTransaction(String merchantId, String transactionId);
}
