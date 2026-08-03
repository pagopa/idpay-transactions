package it.gov.pagopa.idpay.transactions.persistence.sql;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface RewardTransactionSqlRepository
        extends ReactiveCrudRepository<RewardTransactionEntity, String> {
}
