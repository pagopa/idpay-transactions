package it.gov.pagopa.idpay.transactions.persistence.sql;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface RewardTransactionSqlRepository
        extends ReactiveCrudRepository<RewardTransactionEntity, String> {

    @Query("""
    DELETE FROM reward_transactions
    WHERE reward_batch_id = :rewardBatchId
      AND initiative_id = :initiativeId
    RETURNING transaction_id
    """)
    Flux<String> deleteByRewardBatchIdAndInitiativeIdReturningIds(
            @Param("rewardBatchId") String rewardBatchId,
            @Param("initiativeId") String initiativeId
    );
}
