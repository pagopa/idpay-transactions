package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.Report;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface ReportRepository extends ReactiveMongoRepository<Report, String>, ReportSpecificRepository {

    Mono<Report> findByIdAndInitiativeId(String reportId, String initiativeId);
}
