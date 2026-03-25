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
  //aggiungere initiativeId
  Mono<RewardBatch> updateTotals(String merchantId, String rewardBatchId, BatchCountersDTO batchCountersDTO);
    //aggiungere initiativeId
  Mono<RewardBatch> findRewardBatchByIdAndMerchantId(String rewardBatchId, String merchantId);
    //aggiungere initiativeId
  Mono<RewardBatch> findRewardBatchByFilter(String rewardBatchId, String merchantId, PosType posType, String month);
  Flux<RewardBatch> findRewardBatchByMonthBefore(String merchantId, String initiativeId, PosType posType, String month);
    //aggiungere initiativeId
  Mono<RewardBatch> updateStatusAndApprovedAmountCents(String rewardBatchId, String merchantId, RewardBatchStatus rewardBatchStatus, Long approvedAmountCents);
  Flux<RewardBatch> findPreviousEmptyBatches();
}
