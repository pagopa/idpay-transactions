package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.dto.report.Report2RunDto;
import it.gov.pagopa.idpay.transactions.dto.report.ReportGenerateForce;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import jakarta.validation.Valid;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RequestMapping("/idpay/merchant/portal")
public interface ReportController {

    @GetMapping("/initiatives/{initiativeId}/reports")
    Mono<ReportListDTO> getReports(
            @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
            @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
            @PathVariable("initiativeId") String initiativeId,
            @RequestParam(value = "reportType", required = false) ReportType reportType,
            @PageableDefault Pageable pageable
    );

    @GetMapping("/initiatives/{initiativeId}/reports/{reportId}/download")
    Mono<DownloadReportResponseDTO> downloadReports(
            @RequestHeader(value = "x-merchant-id", required = false) String merchantId,
            @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
            @PathVariable("initiativeId") String initiativeId,
            @PathVariable("reportId") String reportId
    );

    @PostMapping("/initiatives/{initiativeId}/reports")
    Mono<ReportDTO> generateReport(@RequestHeader(value = "x-merchant-id", required = false) String merchantId,
                                                       @RequestHeader(value = "x-organization-role", required = false) String organizationRole,
                                                       @PathVariable("initiativeId") String initiativeId,
                                                       @RequestBody @Valid ReportRequest request);

    @PostMapping("/reports/transaction/force")
    Mono<List<Report2RunDto>> forceGenerateReports(@RequestBody ReportGenerateForce reportGenerateForce);


    @PatchMapping("/initiatives/{initiativeId}/reports/{reportId}")
    Mono<ReportDTO> patchReport(@PathVariable("initiativeId") String initiativeId,
                                @PathVariable("reportId") String reportId,
                                @RequestBody PatchReportRequest request);
}
