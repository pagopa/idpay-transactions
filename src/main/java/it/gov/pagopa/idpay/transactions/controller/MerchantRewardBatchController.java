package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import jakarta.validation.Valid;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/merchant/portal")
public interface MerchantRewardBatchController {

  @GetMapping("/initiatives/{initiativeId}/reward-batches")
  Mono<RewardBatchListDTO> getMerchantRewardBatches(
      @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
      @PathVariable("initiativeId") String initiativeId,
      @PageableDefault(sort = "name", direction = Sort.Direction.ASC) Pageable pageable);

  @PostMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/suspended")
  Mono<RewardBatchDTO> suspendTransactions(
          @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
          @PathVariable("initiativeId") String initiativeId,
          @PathVariable("rewardBatchId") String rewardBatchId,
          @RequestBody @Valid TransactionsRequest request);

}
