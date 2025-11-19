package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.BatchType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface RewardBatchRepository extends ReactiveMongoRepository<RewardBatch, String>,
    RewardBatchSpecificRepository {

  Mono<RewardBatch> findByMerchantIdAndPosTypeAndMonthAndBatchType(String merchantId,
      PosType posType, String month, BatchType batchType
  );
}
