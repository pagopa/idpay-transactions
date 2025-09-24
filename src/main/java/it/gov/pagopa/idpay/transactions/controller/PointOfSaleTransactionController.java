package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay")
public interface PointOfSaleTransactionController {

  @GetMapping("/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
  Mono<PointOfSaleTransactionsListDTO> getPointOfSaleTransactions(
      @RequestHeader("x-merchant-id") String merchantId,
      @PathVariable("initiativeId") String initiativeId,
      @PathVariable("pointOfSaleId") String pointOfSaleId,
      @RequestParam(required = false) String productGtin,
      @RequestParam(required = false) String fiscalCode,
      @RequestParam(required = false) String status,
      @PageableDefault(sort = "elaborationDateTime", direction = Sort.Direction.DESC) Pageable pageable);
}
