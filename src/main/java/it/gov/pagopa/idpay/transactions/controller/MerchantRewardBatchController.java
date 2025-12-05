package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchesRequest;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import jakarta.validation.Valid;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/merchant/portal")
public interface MerchantRewardBatchController {

  @GetMapping("/initiatives/{initiativeId}/reward-batches")
  Mono<RewardBatchListDTO> getRewardBatches(
      @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
      @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) String assigneeLevel,
      @PathVariable("initiativeId") String initiativeId,
      @PageableDefault(sort = "month", direction = Sort.Direction.ASC) Pageable pageable);

  @PostMapping("/initiatives/{initiativeId}/reward-batches/{batchId}/send")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  Mono<Void> sendRewardBatches(
      @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
      @PathVariable("initiativeId") String initiativeId,
      @PathVariable("batchId") String batchId
      );

  @PostMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/suspended")
  Mono<RewardBatchDTO> suspendTransactions(
          @PathVariable("initiativeId") String initiativeId,
          @PathVariable("rewardBatchId") String rewardBatchId,
          @RequestBody @Valid TransactionsRequest request);


@PutMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/approved")
Mono<RewardBatch>  rewardBatchConfirmation(
        @PathVariable("initiativeId") String initiativeId,
        @PathVariable("rewardBatchId") String rewardBatchId);

  @PutMapping("/initiatives/{initiativeId}/reward-batches/approved")
  Mono<Void>  rewardBatchConfirmationBatch(
          @PathVariable("initiativeId") String initiativeId,
          @RequestBody  RewardBatchesRequest request);

  @PutMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/generateAndSaveCsv")
  Mono<Void>  generateAndSaveCsv(
          @PathVariable("initiativeId") String initiativeId,
          @PathVariable("rewardBatchId") String rewardBatchId);


  @PostMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/rejected")
  Mono<RewardBatchDTO> rejectTransactions(
          @PathVariable("initiativeId") String initiativeId,
          @PathVariable("rewardBatchId") String rewardBatchId,
          @RequestBody @Valid TransactionsRequest request);


  @PostMapping("/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/transactions/approved")
  Mono<RewardBatchDTO> approvedTransactions(
          @PathVariable("initiativeId") String initiativeId,
          @PathVariable("rewardBatchId") String rewardBatchId,
          @RequestBody @Valid TransactionsRequest request);

  @PostMapping("/initiatives/{initiativeId}/reward-batches/evaluate")
  Mono<Void> evaluatingRewardBatches(
          @RequestBody RewardBatchesRequest rewardBatchIds
  );
}
