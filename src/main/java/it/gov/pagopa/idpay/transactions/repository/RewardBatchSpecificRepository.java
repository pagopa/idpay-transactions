package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardBatchSpecificRepository {
  Flux<RewardBatch> findRewardBatchByMerchantId(String merchantId, String status, String assigneeLevel, Pageable pageable);
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);
  Flux<RewardBatch> findRewardBatch(String status, String assigneeLevel, Pageable pageable);
  Mono<Long> getCount(String merchantId, String status, String assigneeLevel);
  Mono<Long> getCount(String status, String assigneeLevel);
}
