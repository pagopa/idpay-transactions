package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/merchant/portal")
public interface MerchantRewardBatchController {

  @GetMapping("/initiatives/{initiativeId}/reward-batches")
  Mono<RewardBatchListDTO> getMerchantRewardBatches(
      @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) String assigneeLevel,
      @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
      @PathVariable("initiativeId") String initiativeId,
      @PageableDefault(sort = "name", direction = Sort.Direction.ASC) Pageable pageable);

  @PostMapping("/initiatives/{initiativeId}/reward-batches/{batchId}/send")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  Mono<Void> sendRewardBatches(
      @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
      @PathVariable("initiativeId") String initiativeId,
      @PathVariable("batchId") String batchId
      );
}
