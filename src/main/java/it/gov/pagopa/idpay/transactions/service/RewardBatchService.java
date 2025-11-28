package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;


public interface RewardBatchService {

  Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, String month, String businessName);
  Mono<Page<RewardBatch>> getMerchantRewardBatches(String merchantId, Pageable pageable);
  Mono<Page<RewardBatch>> getAllRewardBatches(Pageable pageable);
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);

  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId);

  Mono<Void> sendRewardBatch(String merchantId, String batchId);
  Mono<RewardBatch> suspendTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request);

  Mono<RewardBatch> rejectTransactions(String rewardBatchId, String initiativeId, TransactionsRequest request);
  Mono<RewardBatch> approvedTransactions(String rewardBatchId, TransactionsRequest request, String initiativeId);
}
