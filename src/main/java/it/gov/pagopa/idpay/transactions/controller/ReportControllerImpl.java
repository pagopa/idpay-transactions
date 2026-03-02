package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.*;
import it.gov.pagopa.idpay.transactions.dto.report.Report2RunDto;
import it.gov.pagopa.idpay.transactions.dto.report.ReportGenerateForce;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.service.ReportService;
import it.gov.pagopa.idpay.transactions.dto.mapper.ReportMapper;
import it.gov.pagopa.idpay.transactions.utils.Utilities;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@Slf4j
public class ReportControllerImpl implements ReportController {

    private final ReportService reportService;
    private final ReportMapper reportMapper;

    public ReportControllerImpl(ReportService reportService, ReportMapper reportMapper) {
        this.reportService = reportService;
        this.reportMapper = reportMapper;
    }

    @Override
    public Mono<ReportListDTO> getReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType,
            Pageable pageable
    ) {
        log.info("[GET_REPORTS] Request received for initiative: {}", Utilities.sanitizeString(initiativeId));

        return reportService.getReports(merchantId, organizationRole, initiativeId, reportType, pageable)
                .flatMap(page -> Mono.just(reportMapper.toListDTO(page)));
    }

    @Override
    public Mono<DownloadReportResponseDTO> downloadReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            String reportId
    ) {
        log.info("[DOWNLOAD_REPORT] Request received for initiative: {}, reportId: {}",
                Utilities.sanitizeString(initiativeId),
                Utilities.sanitizeString(reportId));

        return reportService.downloadReports(
                merchantId,
                organizationRole,
                initiativeId,
                reportId
        );
    }


    @Override
    public Mono<ReportDTO> generateReport(String merchantId,
                                          String organizationRole,
                                          String initiativeId,
                                          ReportRequest request
    ) {
            return reportService.generateReport(merchantId, organizationRole, initiativeId, request);
    }


    @Override
    public Mono<ReportDTO> patchReport(String initiativeId,
                                       String reportId,
                                       PatchReportRequest request
    ) {
        return reportService.patchReport(initiativeId, reportId, request);
    }

    @Override
    public Mono<List<Report2RunDto>> forceGenerateReports(ReportGenerateForce reportGenerateForce) {
        return reportService.forceGenerateReports(reportGenerateForce);
    }
}
