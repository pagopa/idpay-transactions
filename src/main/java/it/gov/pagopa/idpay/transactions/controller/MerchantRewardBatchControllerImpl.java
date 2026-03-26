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
import java.time.LocalDate;
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

    if (organizationRole != null) {
        log.info("[GET_REWARD_BATCHES] Request received. Merchant: {}, Role: {}, Initiative: {}",
                validMerchantId != null ? Utilities.sanitizeString(validMerchantId) : "null",
                Utilities.sanitizeString(organizationRole),
                Utilities.sanitizeString(initiativeId));
    } else {
        log.info("[GET_REWARD_BATCHES] Request received. Merchant: {}, Initiative: {}",
                Utilities.sanitizeString(validMerchantId),
                Utilities.sanitizeString(initiativeId));
    }

      log.info("[GET_REWARD_BATCHES] Request received. Merchant: {}, Role: {}, Initiative: {}",
              validMerchantId != null ? Utilities.sanitizeString(validMerchantId) : "null",
              organizationRole != null ? Utilities.sanitizeString(organizationRole) : "null",
              initiativeId != null ? Utilities.sanitizeString(initiativeId) : "null");

    return rewardBatchService.getRewardBatches(validMerchantId, initiativeId, organizationRole, status, assigneeLevel, month, pageable)
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
    log.info("[GET_REWARD_BATCH_BY_ID] Request received. Merchant: {}, InitiativeId: {}, RewardBatchId: {}",
            Utilities.sanitizeString(merchantId), Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(rewardBatchId));
    return getRewardBatchByIdUseCase.execute(merchantId, initiativeId, rewardBatchId)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
    public Mono<Void> sendRewardBatches(String merchantId, String initiativeId, String batchId) {
        log.info("[SEND_REWARD_BATCHES] Merchant {} requested to send batch batchId {}",
                Utilities.sanitizeString(merchantId), Utilities.sanitizeString(batchId));
        return this.rewardBatchService.sendRewardBatch(initiativeId, merchantId, batchId);
    }

  @Override
  public  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String merchantId, String rewardBatchId) {
    log.info("[REWARD_BATCH_CONFIRMATION] Batch confirmation for batch batchId {}",
            Utilities.sanitizeString(rewardBatchId));
    return rewardBatchService.rewardBatchConfirmation(initiativeId, merchantId, rewardBatchId);
  }

  @Override
  public  Mono<Void> rewardBatchConfirmationBatch(String initiativeId, String merchantId, RewardBatchesRequest request) {
    List<String> rewardBatchIds = request.getRewardBatchIds() != null ? request.getRewardBatchIds() : List.of();
    log.info("[REWARD_BATCH_CONFIRMATION_BATCH] Batch confirmation for initiative {} and batchs {}",
            Utilities.sanitizeString(initiativeId), rewardBatchIds);
    return rewardBatchService.rewardBatchConfirmationBatch(initiativeId, merchantId, rewardBatchIds);
  }

    @Override
    public  Mono<Void> rewardBatchDeliveryBatch(String initiativeId, String merchantId, RewardBatchesRequest request) {
        List<String> rewardBatchIds = request.getRewardBatchIds() != null ? request.getRewardBatchIds() : List.of();
        log.info("[REWARD_BATCH_DELIVERY_BATCH] Batch delivery for initiative {} and batchs {}",
                Utilities.sanitizeString(initiativeId), rewardBatchIds);
        return rewardBatchService.rewardBatchDeliveryBatch(initiativeId, merchantId, rewardBatchIds);
    }

  @Override
  public Mono<Void> checkRewardBatchesOutcomes(String initiativeId, RewardBatchesRequest request, String merchantId) {
    List<String> rewardBatchIds = request != null && request.getRewardBatchIds() != null ? request.getRewardBatchIds() : List.of();

    List<String> sanitizedBatchIds = rewardBatchIds.stream()
            .map(Utilities::sanitizeString)
            .toList();

    log.info("[CHECK_REWARD_BATCHES_OUTCOMES] initiative {} rewardBatchIds {}", sanitizeString(initiativeId), sanitizedBatchIds);
    return rewardBatchService.checkRewardBatchesOutcomes(initiativeId, rewardBatchIds, merchantId);
  }

  @Override
  public  Mono<String> generateAndSaveCsv(String initiativeId, String rewardBatchId, String merchantId) {
    log.info("[GENERATE_AND_SAVE_CSV] Generate CSV for initiative {} and batch {}",
            Utilities.sanitizeString(initiativeId), Utilities.sanitizeString(rewardBatchId) );
    return rewardBatchService.generateAndSaveCsv(rewardBatchId, initiativeId, merchantId);
  }


  @Override
  public Mono<RewardBatchDTO> suspendTransactions(String merchantId, String initiativeId, String rewardBatchId, TransactionsRequest request) {

    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();
    String reason = request.getReason();
    if(request.getReason() == null || request.getReason().isEmpty()){
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
              ExceptionCode.REASON_FIELD_IS_MANDATORY,
              ExceptionConstants.ExceptionMessage.REASON_FIELD_IS_MANDATORY);
    }

    log.info(
            "[SUSPEND_TRANSACTIONS] Requested to suspend {} transactions for rewardBatch {} of merchant {} with reason '{}'",
            transactionIds.size(),
            Utilities.sanitizeString(rewardBatchId),
            Utilities.sanitizeString(merchantId),
            Utilities.sanitizeString(reason)
    );

    return rewardBatchService.suspendTransactions(rewardBatchId, merchantId, initiativeId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }


  @Override
  public Mono<RewardBatchDTO> rejectTransactions(String initiativeId, String merchantId, String rewardBatchId, TransactionsRequest request) {

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
            Utilities.sanitizeString(rewardBatchId),
            Utilities.sanitizeString(initiativeId),
            Utilities.sanitizeString(reason)
    );

    return rewardBatchService.rejectTransactions(rewardBatchId, initiativeId, merchantId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
  public Mono<RewardBatchDTO> approvedTransactions(String initiativeId, String merchantId, String rewardBatchId, TransactionsRequest request) {

    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();

    log.info(
            "[APPROVED_TRANSACTIONS] Requested to approve {} transactions for rewardBatch {} of merchant {} for initiative {}",
            transactionIds.size(),
            Utilities.sanitizeString(rewardBatchId),
            Utilities.sanitizeString(merchantId),
            Utilities.sanitizeString(initiativeId)
    );

    return rewardBatchService.approvedTransactions(rewardBatchId, request, merchantId, initiativeId)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
  public Mono<Void> evaluatingRewardBatches(RewardBatchesRequest rewardBatchesRequest, String initiativeId, String merchantId) {
    log.info(
            "[EVALUATING_REWARD_BATCH] Requested to evaluate {}", rewardBatchesRequest.getRewardBatchIds() != null
                    ? rewardBatchesRequest.getRewardBatchIds().stream()
                    .map(Utilities::sanitizeString).toList()
                    : "all reward batches with status SENT"
    );
    return rewardBatchService.evaluatingRewardBatches(rewardBatchesRequest.getRewardBatchIds(), initiativeId, merchantId)
            .then();
  }

  @Override
  public Mono<DownloadRewardBatchResponseDTO> downloadApprovedRewardBatch(String merchantId, String organizationRole, String initiativeId, String rewardBatchId) {

    log.info("[DOWNLOAD_APPROVED_REWARD_BATCH] Requested to download approved reward batch {} for initiative {}",
            Utilities.sanitizeString(rewardBatchId),
            Utilities.sanitizeString(initiativeId));

    return rewardBatchService.downloadApprovedRewardBatchFile(
            merchantId,
            organizationRole,
            initiativeId,
            rewardBatchId
    );
  }

  @Override
  public Mono<RewardBatch> validateRewardBatch(String organizationRole, String initiativeId, String rewardBatchId) {

    log.info(
            "[VALIDATE_REWARD_BATCH] Request to validate rewardBatch {} for initiative {} by role {}",
            Utilities.sanitizeString(rewardBatchId),
            Utilities.sanitizeString(initiativeId),
            Utilities.sanitizeString(organizationRole)
    );

    return rewardBatchService.validateRewardBatch(organizationRole, initiativeId, rewardBatchId);
  }

  @Override
  public Mono<Void> postponeTransaction(String merchantId, String initiativeId, String rewardBatchId, String transactionId, LocalDate initiativeEndDate) {
    log.info(
        "[POSTPONE_TRANSACTION] Merchant {} requested to postpone transaction {} for rewardBatch {} of initiative {}",
        Utilities.sanitizeString(merchantId),
        Utilities.sanitizeString(transactionId),
        Utilities.sanitizeString(rewardBatchId),
        Utilities.sanitizeString(initiativeId)
    );

    return rewardBatchService.postponeTransaction(merchantId, initiativeId, rewardBatchId, transactionId, initiativeEndDate);
  }

  @Override
  public Mono<Void> cancelEmptyRewardBatches(){
    log.info("[CANCEL_EMPTY_BATCHES] Request to delete all empty batches");
    return rewardBatchService.deleteEmptyRewardBatches();
  }
}
