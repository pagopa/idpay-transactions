package it.gov.pagopa.idpay.transactions.persistence.sql;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface RewardBatchSqlRepository extends ReactiveCrudRepository<RewardBatchEntity, String> {

    Mono<RewardBatchEntity> findByIdAndInitiativeId(String id, String initiativeId);

    Mono<RewardBatchEntity> findByIdAndInitiativeIdAndMerchantId(
            String id,
            String initiativeId,
            String merchantId
    );

    Mono<RewardBatchEntity> findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
            String initiativeId,
            String merchantId,
            String posType,
            String month
    );
}
