package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface RewardTransactionWritePort {

    Mono<RewardTransaction> save(RewardTransaction rewardTransaction);

    Mono<Void> deleteById(String transactionId);
}
