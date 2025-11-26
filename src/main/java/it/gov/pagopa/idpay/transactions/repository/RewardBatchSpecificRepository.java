package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface RewardBatchSpecificRepository {
  Flux<RewardBatch> findRewardBatchByMerchantId(String merchantId, Pageable pageable);
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);
  Flux<RewardBatch> findRewardBatch(Pageable pageable);
  Mono<Long> getCount(String merchantId);
  Mono<Long> getCount();
  Mono<Long> updateTransactionsStatus(String rewardBatchId, List<String> transactionIds, RewardBatchTrxStatus newStatus, String reason);
  Mono<RewardBatch> updateTotals(String rewardBatchId, long elaboratedTrxNumber, long updateAmountCents, long rejectedTrxNumber, long suspendedTrxNumber);

  Mono<RewardBatch> findRewardBatchById(String rewardBatchId);
  Mono<RewardBatch> findRewardBatchByFilter(String rewardBatchId, String merchantId, String posType, String month);


}
