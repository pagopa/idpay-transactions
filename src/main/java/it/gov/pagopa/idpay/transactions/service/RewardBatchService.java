package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;

public interface RewardBatchService {

  Mono<RewardBatch> findOrCreateBatch(String merchantId, String posType, String month, String businessName);
  Mono<Page<RewardBatch>> getMerchantRewardBatches(String merchantId, Pageable pageable);
  Mono<Page<RewardBatch>> getAllRewardBatches(Pageable pageable);
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);

  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId);
  Mono<RewardBatch> provaGet(String initiativeId, String rewardBatchId);

  Mono<RewardBatch> provaSave(String initiativeId, String rewardBatchId);

  Mono<RewardBatch> provaSaveAndCreateNewBatch(String initiativeId, String rewardBatchId);

}
