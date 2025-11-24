package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

public interface RewardBatchSpecificRepository {
  Flux<RewardBatch> findRewardBatchByMerchantId(String merchantId, Pageable pageable);
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);
  Flux<RewardBatch> findRewardBatch(Pageable pageable);
  Mono<Long> getCount(String merchantId);
  Mono<Long> getCount();

  Mono<RewardBatch> findRewardBatchById(String reawardBatchId);
  Flux<RewardTransaction> findByFilter(String rewardBatchId, String initiativeI);

}
