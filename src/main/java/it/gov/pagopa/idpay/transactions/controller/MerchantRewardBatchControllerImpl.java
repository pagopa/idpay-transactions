package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchesRequest;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardBatchMapper;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@Slf4j
public class MerchantRewardBatchControllerImpl implements MerchantRewardBatchController{

  private final RewardBatchService rewardBatchService;
  private final RewardBatchMapper rewardBatchMapper;

  public MerchantRewardBatchControllerImpl(RewardBatchService rewardBatchService, RewardBatchMapper rewardBatchMapper){
    this.rewardBatchService = rewardBatchService;
    this.rewardBatchMapper = rewardBatchMapper;
  }

  @Override
  public Mono<RewardBatchListDTO> getRewardBatches(String merchantId, String organizationRole, String status, String assigneeLevel, String initiativeId, Pageable pageable) {

    if (merchantId == null && organizationRole == null) {
      throw new ClientExceptionWithBody(
          HttpStatus.BAD_REQUEST,
          ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS,
          ExceptionMessage.MISSING_TRANSACTIONS_FILTERS
      );
    }

    log.info("[GET_REWARD_BATCHES] Request received. Merchant: {}, Role: {}",
        merchantId != null ? Utilities.sanitizeString(merchantId) : "null",
        organizationRole != null ? Utilities.sanitizeString(organizationRole) : "null");

    return rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel, pageable)
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
    public Mono<Void> sendRewardBatches(String merchantId, String initiativeId, String batchId) {
        log.info("[SEND_REWARD_BATCHES] Merchant {} requested to send batch batchId {}",
                Utilities.sanitizeString(merchantId), Utilities.sanitizeString(batchId));
        return this.rewardBatchService.sendRewardBatch(merchantId, batchId);
    }

  @Override
  public  Mono<RewardBatch> rewardBatchConfirmation(String initiativeId, String rewardBatchId) {
    log.info("[REWARD_BATCH_CONFIRMATION] Batch confirmation fot batch batchId {}",
            Utilities.sanitizeString(rewardBatchId));
    return rewardBatchService.rewardBatchConfirmation(initiativeId, rewardBatchId);
  }

  @Override
  public Mono<RewardBatchDTO> suspendTransactions(String initiativeId, String rewardBatchId, TransactionsRequest request) {

    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();
    String reason = request.getReason();
    if(request.getReason() == null || request.getReason().isEmpty()){
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST,
              ExceptionCode.REASON_FIELD_IS_MANDATORY,
              ExceptionConstants.ExceptionMessage.REASON_FIELD_IS_MANDATORY);
    }

    log.info(
            "[SUSPEND_TRANSACTIONS] Requested to suspend {} transactions for rewardBatch {} of initiative {} with reason '{}'",
            transactionIds.size(),
            Utilities.sanitizeString(rewardBatchId),
            Utilities.sanitizeString(initiativeId),
            Utilities.sanitizeString(reason)
    );

    return rewardBatchService.suspendTransactions(rewardBatchId, initiativeId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }


  @Override
  public Mono<RewardBatchDTO> rejectTransactions(String initiativeId, String rewardBatchId, TransactionsRequest request) {

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

    return rewardBatchService.rejectTransactions(rewardBatchId, initiativeId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
  public Mono<RewardBatchDTO> approvedTransactions(String initiativeId, String rewardBatchId, TransactionsRequest request) {

    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();

    log.info(
            "[APPROVED_TRANSACTIONS] Requested to approve {} transactions for rewardBatch {} of initiative {}",
            transactionIds.size(),
            Utilities.sanitizeString(rewardBatchId),
            Utilities.sanitizeString(initiativeId)
    );

    return rewardBatchService.approvedTransactions(rewardBatchId, request, initiativeId)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
  public Mono<Void> evaluatingRewardBatches(RewardBatchesRequest rewardBatchesRequest) {
    log.info(
            "[EVALUATING_REWARD_BATCH] Requested to evaluate {}", rewardBatchesRequest.getRewardBatchIds() != null
                    ? "reward batches " + rewardBatchesRequest.getRewardBatchIds()
                    : "all reward batches with status SENT"
    );
    return rewardBatchService.evaluatingRewardBatches(rewardBatchesRequest.getRewardBatchIds())
            .then();
  }
}
