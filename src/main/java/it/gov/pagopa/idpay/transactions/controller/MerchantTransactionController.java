package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import org.springframework.data.domain.Pageable;
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
                                                              @RequestBody(required = false) TrxFiltersDTO filters,
                                                              @PageableDefault Pageable pageable);

    @GetMapping("/initiatives/{initiativeId}/transactions/processed/statuses")
    Mono<List<String>> getProcessedTransactionStatuses(
            @RequestHeader(value = "x-organization-role", required = false) String organizationRole);

}
