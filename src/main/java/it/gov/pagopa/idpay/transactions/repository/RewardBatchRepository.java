package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface RewardBatchRepository extends ReactiveMongoRepository<RewardBatch, String>,
    RewardBatchSpecificRepository {

  Mono<RewardBatch> findByMerchantIdAndPosTypeAndMonth(String merchantId,
                                                       PosType posType, String month
  );

  Mono<RewardBatch> findByIdAndStatus(String rewardBatchId, RewardBatchStatus rewardBatchTrxStatus);
}
