package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RequestMapping("/idpay/merchant/portal")
public interface MerchantTransactionController {
    @GetMapping("/initiatives/{initiativeId}/transactions/processed")
    Mono<MerchantTransactionsListDTO> getMerchantTransactions(@RequestHeader("x-merchant-id") String merchantId,
                                                              @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
                                                              @PathVariable("initiativeId") String initiativeId,
                                                              @RequestParam(required = false) String fiscalCode,
                                                              @RequestParam(required = false) String status,
                                                              @RequestParam(required = false) String rewardBatchId,
                                                              @RequestParam(required = false) String rewardBatchTrxStatus,
                                                              @RequestParam(required = false) String pointOfSaleId,
                                                              @PageableDefault(sort="rewardBatchTrxStatus", direction = Sort.Direction.DESC) Pageable pageable);

    @GetMapping("/initiatives/{initiativeId}/transactions/processed/statuses")
    Mono<List<String>> getProcessedTransactionStatuses(
            @RequestHeader(value = "x-organization-role", required = false) String organizationRole);

}
