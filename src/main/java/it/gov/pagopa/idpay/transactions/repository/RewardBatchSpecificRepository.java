package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RewardBatchSpecificRepository {
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);
  Flux<RewardBatch> findRewardBatchesCombined(String merchantId, String status, String assigneeLevel, boolean isOperator, Pageable pageable);
  Mono<Long> getCountCombined(String merchantId, String status, String assigneeLevel, boolean isOperator);
}
