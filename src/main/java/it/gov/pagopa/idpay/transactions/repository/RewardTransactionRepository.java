package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface RewardTransactionRepository extends ReactiveMongoRepository<RewardTransaction,String> {
}
