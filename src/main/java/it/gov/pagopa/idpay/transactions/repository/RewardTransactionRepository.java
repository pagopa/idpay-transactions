package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Query;
import reactor.core.publisher.Flux;

public interface RewardTransactionRepository extends ReactiveMongoRepository<RewardTransaction,String>, RewardTransactionSpecificRepository {

    @Query("{ 'rewardBatchId': ?0, 'initiatives': ?1 }")
    Flux<RewardTransaction> findByRewardBatchIdAndInitiativeId(String rewardBatchId, String initiativeId);
}
