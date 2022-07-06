package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Mono;

public interface RewardTransactionService {
    Mono<RewardTransaction> save(RewardTransaction transaction);
}
