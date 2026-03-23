package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardBatchRepository extends ReactiveMongoRepository<RewardBatch, String>,
    RewardBatchSpecificRepository {

  Mono<RewardBatch> findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(String initiativeId, String merchantId,
                                                                      PosType posType, String month);

  Mono<RewardBatch> findByIdAndMerchantIdAndStatus(String rewardBatchId, String merchantId, RewardBatchStatus rewardBatchTrxStatus);

  Flux<RewardBatch> findByStatusAndInitiativeId(RewardBatchStatus rewardBatchStatus, String initiativeId);
  Flux<RewardBatch> findByMerchantIdAndInitiativeIdAndPosType(String merchantId, String initiativeId, PosType posType);

  Mono<RewardBatch> findByMerchantIdAndInitiativeIdAndId(String merchantId, String initiativeId, String rewardBatchId);
}
