package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/merchant/portal")
public interface MerchantRewardBatchController {

  @GetMapping("/initiatives/{initiativeId}/reward-batches")
  Mono<RewardBatchListDTO> getMerchantRewardBatches(
      @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
      @PathVariable("initiativeId") String initiativeId,
      @PageableDefault(sort = "name", direction = Sort.Direction.ASC) Pageable pageable);

@PutMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/approved")
Mono<RewardBatch>  rewardBatchConfirmation(
        @PathVariable("initiativeId") String initiativeId,
        @PathVariable("rewardBatchId") String rewardBatchId);


@GetMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/approved/provaGet")
Mono<RewardBatch> provaGet(
        @PathVariable("initiativeId") String initiativeId,
        @PathVariable("rewardBatchId") String rewardBatchId);

@PutMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/approved/provaSave")
Mono<RewardBatch> provaSave(
        @PathVariable("initiativeId") String initiativeId,
        @PathVariable("rewardBatchId") String rewardBatchId);

  @PutMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/approved/provaSaveAndCreateNewBatch")
  Mono<RewardBatch> provaSaveAndCreateNewBatch(
          @PathVariable("initiativeId") String initiativeId,
          @PathVariable("rewardBatchId") String rewardBatchId);
}
