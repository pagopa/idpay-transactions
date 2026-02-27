package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReportSpecificRepository {
    Flux<Report> findReportsCombined(String merchantId, String organizationRole, String initiativeId, ReportType reportType, Pageable pageable);
    Mono<Long> countReportsCombined(String merchantId, String organizationRole, String initiativeId, ReportType reportType);
}
