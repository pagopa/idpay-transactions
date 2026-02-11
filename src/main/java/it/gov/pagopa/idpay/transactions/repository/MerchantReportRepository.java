package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.MerchantReport;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface MerchantReportRepository extends ReactiveMongoRepository<MerchantReport,String> {
}
