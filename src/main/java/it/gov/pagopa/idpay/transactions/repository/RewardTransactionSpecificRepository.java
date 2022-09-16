package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;

public interface RewardTransactionSpecificRepository {
    Flux<RewardTransaction> findAllInRange(LocalDateTime start, LocalDateTime end);
}
