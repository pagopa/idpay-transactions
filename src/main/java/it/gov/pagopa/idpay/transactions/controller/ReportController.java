package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.PatchReportRequest;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportListDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import jakarta.validation.Valid;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/merchant/portal")
public interface ReportController {

    @GetMapping("/initiatives/{initiativeId}/reports")
    Mono<ReportListDTO> getTransactionsReports(
            @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
            @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
            @PathVariable("initiativeId") String initiativeId,
            @PageableDefault Pageable pageable
    );

    @PostMapping("/initiatives/{initiativeId}/reports")
    Mono<ReportDTO> generateReport(@RequestHeader("x-merchant-id") String merchantId,
                                                       @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
                                                       @PathVariable("initiativeId") String initiativeId,
                                                       @RequestBody @Valid ReportRequest request);


    @PatchMapping("/initiatives/{initiativeId}/reports/{reportId}")
    Mono<ReportDTO> patchReport(@PathVariable("initiativeId") String initiativeId,
                                @PathVariable("reportId") String reportId,
                                @RequestBody PatchReportRequest request);
}
