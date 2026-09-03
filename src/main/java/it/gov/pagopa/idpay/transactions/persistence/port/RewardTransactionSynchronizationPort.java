package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface RewardTransactionSynchronizationPort {

    Mono<RewardTransaction> upsert(RewardTransaction transaction);

    Mono<RewardTransaction> upsertRefundedAndDetach(RewardTransaction transaction);
}
