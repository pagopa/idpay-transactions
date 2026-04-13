package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchesRequest;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardBatchMapper;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.usecase.rewardbatch.GetRewardBatchByIdUseCase;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static it.gov.pagopa.idpay.transactions.utils.Utilities.sanitizeString;

@RestController
@Slf4j
public class MerchantRewardBatchControllerImpl implements MerchantRewardBatchController{

  private final RewardBatchService rewardBatchService;
  private final RewardBatchMapper rewardBatchMapper;
  private final GetRewardBatchByIdUseCase getRewardBatchByIdUseCase;

  public MerchantRewardBatchControllerImpl(RewardBatchService rewardBatchService, RewardBatchMapper rewardBatchMapper, GetRewardBatchByIdUseCase getRewardBatchByIdUseCase){
    this.rewardBatchService = rewardBatchService;
    this.rewardBatchMapper = rewardBatchMapper;
    this.getRewardBatchByIdUseCase = getRewardBatchByIdUseCase;
  }

  @Override
  public Mono<RewardBatchListDTO> getRewardBatches(String merchantId, String organizationRole, String status, String assigneeLevel, String month, String merchantIdFilter, String initiativeId, Pageable pageable) {

    if (merchantId == null && organizationRole == null) {
      throw new ClientExceptionWithBody(
          HttpStatus.BAD_REQUEST,
          ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS,
          ExceptionMessage.MISSING_TRANSACTIONS_FILTERS
      );
    }

    String validMerchantId = merchantId != null ? merchantId : merchantIdFilter;
    String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    String sanitizeOrganizationRole = organizationRole == null ? null : Utilities.sanitizeString(organizationRole);
    String sanitizeStatus = status == null ? null : Utilities.sanitizeString(status);
    String sanitizeAssigneeLevel = assigneeLevel == null ? null : Utilities.sanitizeString(assigneeLevel);

    if (organizationRole != null) {
        log.info("[GET_REWARD_BATCHES] Request received. Merchant: {}, Role: {}, Initiative: {}",
                validMerchantId != null ? Utilities.sanitizeString(validMerchantId) : "null",
                sanitizeOrganizationRole,
                sanitizeInitiativeId);
    } else {
        log.info("[GET_REWARD_BATCHES] Request received. Merchant: {}, Initiative: {}",
                Utilities.sanitizeString(validMerchantId),
                sanitizeInitiativeId);
    }

      log.info("[GET_REWARD_BATCHES] Request received. Merchant: {}, Role: {}, Initiative: {}",
              validMerchantId != null ? Utilities.sanitizeString(validMerchantId) : "null",
              organizationRole != null ? sanitizeOrganizationRole : "null",
              initiativeId != null ? sanitizeInitiativeId : "null");

    return rewardBatchService.getRewardBatches(validMerchantId, sanitizeInitiativeId, sanitizeOrganizationRole, sanitizeStatus, sanitizeAssigneeLevel, month, pageable)
        .flatMap(page ->
            Flux.fromIterable(page.getContent())
                .flatMapSequential(rewardBatchMapper::toDTO)
                .collectList()
                .map(dtoList -> new RewardBatchListDTO(
                    dtoList,
                    page.getNumber(),
                    page.getSize(),
                    (int) page.getTotalElements(),
                    page.getTotalPages()
                ))
        );
  }

  @Override
  public Mono<RewardBatchDTO> getRewardBatchById(String merchantId, String initiativeId, String rewardBatchId) {
      String sanitizeMerchantId = merchantId == null ? null : Utilities.sanitizeString(merchantId);
      String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
      String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
    log.info("[GET_REWARD_BATCH_BY_ID] Request received. Merchant: {}, InitiativeId: {}, RewardBatchId: {}",
            sanitizeMerchantId, sanitizeInitiativeId, sanitizeRewardBatchId);
    return getRewardBatchByIdUseCase.execute(sanitizeMerchantId, sanitizeInitiativeId, sanitizeRewardBatchId)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
    public Mono<Void> sendRewardBatches(String merchantId, String initiativeId, String batchId) {
      String sanitizeMerchantId = merchantId == null ? null : Utilities.sanitizeString(merchantId);
      String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
      String sanitizeBatchId = batchId == null ? null : Utilities.sanitizeString(batchId);
        log.info("[SEND_REWARD_BATCHES] Merchant {}, Initiative {} requested to send batch batchId {}",
                sanitizeMerchantId, sanitizeInitiativeId, sanitizeBatchId);
        return this.rewardBatchService.sendRewardBatch(sanitizeInitiativeId, sanitizeMerchantId, sanitizeBatchId);
    }

  @Override
  public  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId) {
      String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
      String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
    log.info("[REWARD_BATCH_CONFIRMATION] Batch confirmation for initiative: {} and batch batchId {}",
            sanitizeInitiativeId, sanitizeRewardBatchId);
    return rewardBatchService.rewardBatchConfirmation(sanitizeInitiativeId, sanitizeRewardBatchId);
  }

  @Override
  public  Mono<Void> rewardBatchConfirmationBatch(String initiativeId, RewardBatchesRequest request) {
    List<String> rewardBatchIds = request.getRewardBatchIds() != null ? request.getRewardBatchIds() : List.of();
      List<String> sanitizedBatchIds = rewardBatchIds.stream()
              .map(Utilities::sanitizeString)
              .toList();
      String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    log.info("[REWARD_BATCH_CONFIRMATION_BATCH] Batch confirmation for initiative {} and batchs {}",
            sanitizeInitiativeId, sanitizedBatchIds);
    return rewardBatchService.rewardBatchConfirmationBatch(sanitizeInitiativeId, sanitizedBatchIds);
  }

    @Override
    public  Mono<Void> rewardBatchDeliveryBatch(String initiativeId, RewardBatchesRequest request) {
        List<String> rewardBatchIds = request.getRewardBatchIds() != null ? request.getRewardBatchIds() : List.of();
        List<String> sanitizedBatchIds = rewardBatchIds.stream()
                .map(Utilities::sanitizeString)
                .toList();
        String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
        log.info("[REWARD_BATCH_DELIVERY_BATCH] Batch delivery for initiative {} and batchs {}",
                sanitizeInitiativeId, sanitizedBatchIds);
        return rewardBatchService.rewardBatchDeliveryBatch(sanitizeInitiativeId, rewardBatchIds);
    }

  @Override
  public Mono<Void> checkRewardBatchesOutcomes(String initiativeId, RewardBatchesRequest request) {
    List<String> rewardBatchIds = request != null && request.getRewardBatchIds() != null ? request.getRewardBatchIds() : List.of();
    String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    List<String> sanitizedBatchIds = rewardBatchIds.stream()
            .map(Utilities::sanitizeString)
            .toList();

    log.info("[CHECK_REWARD_BATCHES_OUTCOMES] initiative {} rewardBatchIds {}", sanitizeInitiativeId, sanitizedBatchIds);
    return rewardBatchService.checkRewardBatchesOutcomes(sanitizeInitiativeId, sanitizedBatchIds);
  }

  @Override
  public  Mono<String> generateAndSaveCsv(String initiativeId, String rewardBatchId, String merchantId) {
    log.info("[GENERATE_AND_SAVE_CSV] Generate CSV for initiative {} and batch {}",
            Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(rewardBatchId) );
    return rewardBatchService.generateAndSaveCsv(rewardBatchId, initiativeId, merchantId);
  }


  @Override
  public Mono<RewardBatchDTO> suspendTransactions(String initiativeId, String rewardBatchId, TransactionsRequest request) {
    String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();
    String reason = request.getReason();
    if(request.getReason() == null || request.getReason().isEmpty()){
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
              ExceptionCode.REASON_FIELD_IS_MANDATORY,
              ExceptionConstants.ExceptionMessage.REASON_FIELD_IS_MANDATORY);
    }

    log.info(
            "[SUSPEND_TRANSACTIONS] Requested to suspend {} transactions for rewardBatch {}  with reason '{}' and initiative: {}",
            transactionIds.size(),
            sanitizeRewardBatchId,
            Utilities.sanitizeString(reason),
            sanitizeInitiativeId
    );

    return rewardBatchService.suspendTransactions(sanitizeRewardBatchId, sanitizeInitiativeId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }


  @Override
  public Mono<RewardBatchDTO> rejectTransactions(String initiativeId, String rewardBatchId, TransactionsRequest request) {
    String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();
    String reason = request.getReason();

    if(request.getReason() == null || request.getReason().isEmpty()){
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
              ExceptionCode.REASON_FIELD_IS_MANDATORY,
              ExceptionConstants.ExceptionMessage.REASON_FIELD_IS_MANDATORY);
    }

    log.info(
            "[REJECT_TRANSACTIONS] Requested to rejected {} transactions for rewardBatch {} of initiative {} with reason '{}'",
            transactionIds.size(),
            sanitizeRewardBatchId,
            sanitizeInitiativeId,
            Utilities.sanitizeString(reason)
    );

    return rewardBatchService.rejectTransactions(sanitizeRewardBatchId, sanitizeInitiativeId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
  public Mono<RewardBatchDTO> approvedTransactions(String initiativeId, String rewardBatchId, TransactionsRequest request) {
    String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();

    log.info(
            "[APPROVED_TRANSACTIONS] Requested to approve {} transactions for rewardBatch {} for initiative {}",
            transactionIds.size(),
            sanitizeRewardBatchId,
            sanitizeInitiativeId
    );

    return rewardBatchService.approvedTransactions(sanitizeRewardBatchId, request, sanitizeInitiativeId)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
  public Mono<Void> evaluatingRewardBatches(RewardBatchesRequest rewardBatchesRequest, String initiativeId) {
    String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    log.info(
            "[EVALUATING_REWARD_BATCH] Requested to evaluate {}", rewardBatchesRequest.getRewardBatchIds() != null
                    ? rewardBatchesRequest.getRewardBatchIds().stream()
                    .map(Utilities::sanitizeString).toList()
                    : "all reward batches with status SENT"
    );
    return rewardBatchService.evaluatingRewardBatches(rewardBatchesRequest.getRewardBatchIds(), sanitizeInitiativeId)
            .then();
  }

  @Override
  public Mono<DownloadRewardBatchResponseDTO> downloadApprovedRewardBatch(String merchantId, String organizationRole, String initiativeId, String rewardBatchId) {
    String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
    String sanitizeOrganizationRole = organizationRole == null ? null : Utilities.sanitizeString(organizationRole);
    String sanitizeMerchantId = merchantId == null ? null : Utilities.sanitizeString(merchantId);
    String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
    log.info("[DOWNLOAD_APPROVED_REWARD_BATCH] Requested to download approved reward batch {} for initiative {} for merchant {}",
            sanitizeRewardBatchId,
            sanitizeInitiativeId,
            sanitizeMerchantId);

    return rewardBatchService.downloadApprovedRewardBatchFile(
            sanitizeMerchantId,
            sanitizeOrganizationRole,
            sanitizeInitiativeId,
            sanitizeRewardBatchId
    );
  }

  @Override
  public Mono<RewardBatch> validateRewardBatch(String organizationRole, String initiativeId, String rewardBatchId) {
      String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
      String sanitizeOrganizationRole = organizationRole == null ? null : Utilities.sanitizeString(organizationRole);
      String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
    log.info(
            "[VALIDATE_REWARD_BATCH] Request to validate rewardBatch {} for initiative {} by role {}",
            sanitizeRewardBatchId,
            sanitizeInitiativeId,
            sanitizeOrganizationRole
    );

    return rewardBatchService.validateRewardBatch(sanitizeOrganizationRole, sanitizeInitiativeId, sanitizeRewardBatchId);
  }

  @Override
  public Mono<Void> postponeTransaction(String merchantId, String initiativeId, String rewardBatchId, String transactionId, Instant initiativeEndDate) {
      String sanitizeInitiativeId = initiativeId == null ? null : Utilities.sanitizeString(initiativeId);
      String sanitizeMerchantId = merchantId == null ? null : Utilities.sanitizeString(merchantId);
      String sanitizeRewardBatchId = rewardBatchId == null ? null : Utilities.sanitizeString(rewardBatchId);
      String sanitizeTransactionId = transactionId == null ? null : Utilities.sanitizeString(transactionId);
      log.info(
        "[POSTPONE_TRANSACTION] Merchant {} requested to postpone transaction {} for rewardBatch {} of initiative {}",
        sanitizeMerchantId,
        sanitizeTransactionId,
        sanitizeRewardBatchId,
        sanitizeInitiativeId
    );

    return rewardBatchService.postponeTransaction(sanitizeMerchantId, sanitizeInitiativeId, sanitizeRewardBatchId, sanitizeTransactionId, initiativeEndDate);
  }

  @Override
  public Mono<Void> cancelEmptyRewardBatches(){
    log.info("[CANCEL_EMPTY_BATCHES] Request to delete all empty batches");
    return rewardBatchService.deleteEmptyRewardBatches();
  }
}
