package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.DownloadRewardBatchResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface RewardBatchService {

  Mono<RewardBatch> findOrCreateBatch(String initiativeId, String merchantId, PosType posType, String month, String businessName);
  Mono<Page<RewardBatch>> getRewardBatches(String merchantId, String initiativeId, String organizationRole, String status, String assigneeLevel, String month, Pageable pageable);
  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String merchantId, String rewardBatchId);

  Mono<Void> rewardBatchConfirmationBatch(String initiativeId, String merchantId, List<String> rewardBatchIds);

  Mono<Void> rewardBatchDeliveryBatch(String initiativeId, String merchantId, List<String> rewardBatchIds);
  Mono<RewardBatch> updateBatch(RewardBatch batch, InvitaliaOutcomeResponseDTO response);
  Mono<Void> checkRewardBatchesOutcomes(String initiativeId, List<String> rewardBatchIds, String merchantId);
  Mono<String> generateAndSaveCsv(String rewardBatchId, String initiativeId, String merchantId);

  Mono<Void> sendRewardBatch(String initiativeId, String merchantId, String batchId);
  Mono<RewardBatch> suspendTransactions(String rewardBatchId, String merchantId, String initiativeId, TransactionsRequest request);

  Mono<RewardBatch> rejectTransactions(String rewardBatchId, String initiativeId, String merchantId, TransactionsRequest request);
  Mono<RewardBatch> approvedTransactions(String rewardBatchId, TransactionsRequest request, String merchantId, String initiativeId);
  Mono<RewardBatch> validateRewardBatch(String organizationRole, String initiativeId, String rewardBatchId);

  Mono<Long> evaluatingRewardBatches(List<String> rewardBatchesRequest, String initiativeId, String merchantId);

  Mono<DownloadRewardBatchResponseDTO> downloadApprovedRewardBatchFile(String merchantId, String organizationRole, String initiativeId, String rewardBatchId);

  Mono<Void> postponeTransaction(String merchantId, String initiativeId, String rewardBatchId, String transactionId, LocalDate initiativeEndDate);

  Mono<Void> deleteEmptyRewardBatches();
}
