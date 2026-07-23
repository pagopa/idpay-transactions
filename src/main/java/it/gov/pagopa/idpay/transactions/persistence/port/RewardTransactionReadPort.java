package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface RewardTransactionReadPort {

    Mono<RewardTransaction> findById(String transactionId);
}
