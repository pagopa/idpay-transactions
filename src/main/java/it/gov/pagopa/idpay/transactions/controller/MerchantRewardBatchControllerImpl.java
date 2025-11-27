package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardBatchMapper;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
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
  public Mono<RewardBatchListDTO> getMerchantRewardBatches(String merchantId, String initiativeId, Pageable pageable) {
    if(merchantId!=null) {
      log.info("[GET_MERCHANT_REWARD_BATCHES] Merchant {} requested to retrieve reward batches", Utilities.sanitizeString(merchantId));
      return this.rewardBatchService.getMerchantRewardBatches(merchantId, pageable)
          .flatMap(page ->
              Flux.fromIterable(page.getContent())
                  .flatMapSequential(rewardBatchMapper::toDTO)
                  .collectList()
                  .map(dtoList -> new RewardBatchListDTO(
                      dtoList,
                      page.getNumber(),
                      page.getSize(),
                      (int) page.getTotalElements(),
                      page.getTotalPages()))
          );
    }else{
      log.info("[GET_ALL_REWARD_BATCHES] Received a request to retrieve all reward batches");
      return this.rewardBatchService.getAllRewardBatches(pageable)
          .flatMap(page ->
              Flux.fromIterable(page.getContent())
                  .flatMapSequential(rewardBatchMapper::toDTO)
                  .collectList()
                  .map(dtoList -> new RewardBatchListDTO(
                      dtoList,
                      page.getNumber(),
                      page.getSize(),
                      (int) page.getTotalElements(),
                      page.getTotalPages()))
          );
    }
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
  public Mono<RewardBatchDTO> suspendTransactions(String merchantId, String initiativeId, String rewardBatchId, TransactionsRequest request) {

    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();
    String reason = request.getReason();

    log.info(
            "[SUSPEND_TRANSACTIONS] Merchant {} requested to suspend {} transactions for rewardBatch {} of initiative {} with reason '{}'",
            Utilities.sanitizeString(merchantId),
            transactionIds.size(),
            rewardBatchId,
            initiativeId,
            Utilities.sanitizeString(reason)
    );

    return rewardBatchService.suspendTransactions(rewardBatchId, initiativeId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }


  @Override
  public Mono<RewardBatchDTO> rejectTransactions(String merchantId, String initiativeId, String rewardBatchId, TransactionsRequest request) {

    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();
    String reason = request.getReason();

    log.info(
            "[SUSPEND_TRANSACTIONS] Merchant {} requested to suspend {} transactions for rewardBatch {} of initiative {} with reason '{}'",
            Utilities.sanitizeString(merchantId),
            transactionIds.size(),
            rewardBatchId,
            initiativeId,
            Utilities.sanitizeString(reason)
    );

    return rewardBatchService.rejectTransactions(rewardBatchId, initiativeId, request)
            .flatMap(rewardBatchMapper::toDTO);
  }

  @Override
  public Mono<RewardBatchDTO> approvedTransactions(String merchantId, String initiativeId, String rewardBatchId, TransactionsRequest request) {

    List<String> transactionIds = request.getTransactionIds() != null ? request.getTransactionIds() : List.of();

    log.info(
            "[APPROVED_TRANSACTIONS] Merchant {} requested to suspend {} transactions for rewardBatch {} of initiative {}",
            Utilities.sanitizeString(merchantId),
            transactionIds.size(),
            rewardBatchId,
            initiativeId
    );

    return rewardBatchService.approvedTransactions(rewardBatchId, request, initiativeId, merchantId)
            .flatMap(rewardBatchMapper::toDTO);
  }
}
