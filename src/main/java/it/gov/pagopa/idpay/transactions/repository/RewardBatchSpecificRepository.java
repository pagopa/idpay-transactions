package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public interface RewardBatchSpecificRepository {
  Flux<RewardBatch> findRewardBatchesCombined(String merchantId, String initiativeId, String status, String assigneeLevel, String month, boolean isOperator, Pageable pageable);
  Mono<Long> getCountCombined(String merchantId, String initiativeId, String status, String assigneeLevel, String month, boolean isOperator);
  Mono<RewardBatch> updateTotals(String initiativeId, String rewardBatchId, BatchCountersDTO batchCountersDTO);
  Mono<RewardBatch> findRewardBatchByIdAndInitiativeId(String rewardBatchId, String initiativeId);
  Flux<RewardBatch> findRewardBatchByMonthBefore(String merchantId, String initiativeId, PosType posType, String month);
  Mono<RewardBatch> updateStatusAndApprovedAmountCents(String rewardBatchId, RewardBatchStatus rewardBatchStatus, Long approvedAmountCents, String initiativeId);
  Flux<RewardBatch> findPreviousEmptyBatches();
}
