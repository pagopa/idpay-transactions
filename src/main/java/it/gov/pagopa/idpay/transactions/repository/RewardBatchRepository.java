package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface RewardBatchRepository extends ReactiveMongoRepository<RewardBatch, String>,
    RewardBatchSpecificRepository {

  Mono<RewardBatch> findByMerchantIdAndPosTypeAndMonth(String merchantId,
      String posType, String month
  );
}
