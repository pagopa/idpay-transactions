package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface RewardBatchSpecificRepository {
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);
  Mono<RewardBatch> decrementTotals(String batchId, long accruedAmountCents);
  Mono<RewardBatch> moveSuspendToNewBatch(String oldBatchId, String newBatchId, long accruedAmountCents);
  Flux<RewardBatch> findRewardBatchesCombined(String merchantId, String status, String assigneeLevel, String month, boolean isOperator, Pageable pageable);
  Mono<Long> getCountCombined(String merchantId, String status, String assigneeLevel, String month, boolean isOperator);
  Mono<Long> updateTransactionsStatus(String rewardBatchId, List<String> transactionIds, RewardBatchTrxStatus newStatus, List<ReasonDTO> reasons);
  Mono<RewardBatch> updateTotals(String rewardBatchId, long elaboratedTrxNumber, long updateAmountCents, long suspendedAmountCents, long rejectedTrxNumber, long suspendedTrxNumber);
  Mono<RewardBatch> findRewardBatchById(String rewardBatchId);
  Mono<RewardBatch> findRewardBatchByFilter(String rewardBatchId, String merchantId, PosType posType, String month);
  Flux<RewardBatch> findRewardBatchByStatus(RewardBatchStatus rewardBatchStatus);
  Flux<RewardBatch> findRewardBatchByMonthBefore(String merchantId, PosType posType, String month);

  Mono<RewardBatch> updateStatusAndApprovedAmountCents(String rewardBatchId, RewardBatchStatus rewardBatchStatus, Long approvedAmountCents);
  Flux<RewardBatch> findPreviousEmptyBatches();
}
