package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

public interface RewardBatchService {

  Mono<RewardBatch> findOrCreateBatch(String merchantId, PosType posType, String month, String businessName);
  Mono<Page<RewardBatch>> getRewardBatches(String merchantId, String organizationRole, String status, String assigneeLevel, Pageable pageable);
  Mono<RewardBatch> incrementTotals(String batchId, long accruedAmountCents);
  Mono<RewardBatch> decrementTotals(String batchId, long accruedAmountCents);
  Mono<Void> sendRewardBatch(String merchantId, String batchId);
}
