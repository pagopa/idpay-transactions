package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.DownloadReportResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.PatchReportRequest;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.dto.report.Report2RunDto;
import it.gov.pagopa.idpay.transactions.dto.report.ReportGenerateForce;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ReportService {
    Mono<Page<Report>> getReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType,
            Pageable pageable
    );

    Mono<Page<Report>> getTransactionsReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            Pageable pageable);

    Mono<Page<Report>> getUserDetailsReports(
            String organizationRole,
            String initiativeId,
            Pageable pageable
    );

    Mono<ReportDTO> generateReport(String merchantId,
                                   String organizationRole,
                                   String initiativeId,
                                   ReportRequest request);

    Mono<ReportDTO> patchReport(String initiativeId,
                                String reportId,
                                PatchReportRequest request);

    Mono<DownloadReportResponseDTO> downloadTransactionsReport(
            String merchantId,
            String organizationRole,
            String initiativeId,
            String reportId
    );


    Mono<List<Report2RunDto>> forceGenerateReports(ReportGenerateForce reportGenerateForce);
}
