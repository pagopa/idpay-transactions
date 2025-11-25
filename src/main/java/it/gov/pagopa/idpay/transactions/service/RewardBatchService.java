package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface RewardBatchService {

  Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, String month, String businessName);
  Mono<Page<RewardBatch>> getMerchantRewardBatches(String merchantId, Pageable pageable);
  Mono<Page<RewardBatch>> getAllRewardBatches(Pageable pageable);
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);

  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId);

  Mono<RewardBatch> suspendTransactions(String rewardBatchId, TransactionsRequest request);
  Mono<Long> updateTransactionsStatus(String rewardBatchId, List<String> transactionIds, RewardBatchTrxStatus newStatus, String reason);
}
