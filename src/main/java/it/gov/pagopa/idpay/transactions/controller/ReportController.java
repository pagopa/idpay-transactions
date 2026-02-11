package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.ReportListDTO;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/merchant/portal")
public interface ReportController {

    @GetMapping("/initiatives/{initiativeId}/transactions/reports")
    Mono<ReportListDTO> getTransactionsReports(
            @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
            @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
            @RequestParam(value = "rewardBatchAssignee", required = false) String rewardBatchAssignee,
            @PathVariable("initiativeId") String initiativeId,
            @PageableDefault Pageable pageable
    );
}
