package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/merchant/portal")
public interface MerchantRewardBatchController {

  @GetMapping("/initiatives/{initiativeId}/reward-batches")
  Mono<RewardBatchListDTO> getMerchantRewardBatches(
      @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
      @PathVariable("initiativeId") String initiativeId,
      @PageableDefault(sort = "name", direction = Sort.Direction.ASC) Pageable pageable);
}
