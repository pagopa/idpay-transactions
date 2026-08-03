package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.model.Report;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReportPersistencePort {

    Flux<Report> findReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType,
            Pageable pageable
    );

    Mono<Long> countReports(
            String merchantId,
            String organizationRole,
            String initiativeId,
            ReportType reportType
    );

    Mono<Report> save(Report report);

    Mono<Report> findByIdAndInitiativeId(String reportId, String initiativeId);

    Mono<Report> findByIdAndInitiativeIdAndMerchantId(
            String reportId,
            String initiativeId,
            String merchantId
    );

    Flux<Report> findAllById(Iterable<String> reportIds);
}
