package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardBatchMapper;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
  public Mono<RewardBatchListDTO> getMerchantRewardBatches(String merchantId, String organizationRole, String status, String assigneeLevel, String initiativeId, Pageable pageable) {

    if (merchantId != null) {
      log.info("[GET_MERCHANT_REWARD_BATCHES] Merchant {} requested to retrieve reward batches",
          Utilities.sanitizeString(merchantId));

      return this.rewardBatchService.getMerchantRewardBatches(merchantId, status, assigneeLevel, pageable)
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

    } else if (organizationRole != null){
      log.info("[GET_ALL_REWARD_BATCHES] Received a request to retrieve all reward batches");

      return this.rewardBatchService.getAllRewardBatches(status, assigneeLevel, organizationRole, pageable)
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
    else {
      throw new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ExceptionMessage.MISSING_TRANSACTIONS_FILTERS);
    }
  }

  @Override
    public Mono<Void> sendRewardBatches(String merchantId, String initiativeId, String batchId) {
        log.info("[SEND_REWARD_BATCHES] Merchant {} requested to send batch batchId {}",
                Utilities.sanitizeString(merchantId), Utilities.sanitizeString(batchId));
        return this.rewardBatchService.sendRewardBatch(merchantId, batchId);
    }
}
