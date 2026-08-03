package it.gov.pagopa.idpay.transactions.persistence.sql;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface ReportSqlRepository extends ReactiveCrudRepository<ReportEntity, String> {
}
