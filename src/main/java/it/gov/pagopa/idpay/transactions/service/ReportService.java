package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.dto.PatchReportRequest;
import it.gov.pagopa.idpay.transactions.dto.ReportDTO;
import it.gov.pagopa.idpay.transactions.dto.ReportRequest;
import it.gov.pagopa.idpay.transactions.model.Report;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

public interface ReportService {
    Mono<Page<Report>> getTransactionsReports(String merchantId, String organizationRole, String initiativeId, Pageable pageable);

    Mono<ReportDTO> generateReport(String merchantId,
                                   String organizationRole,
                                   String initiativeId,
                                   ReportRequest request);

    Mono<ReportDTO> patchReport(String initiativeId,
                                String reportId,
                                PatchReportRequest request);
}
