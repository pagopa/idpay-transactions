package it.gov.pagopa.idpay.transactions.data.factory;

import it.gov.pagopa.idpay.transactions.model.Report;
import reactor.core.publisher.Mono;

public interface DataFactoryService {
    Mono<String> triggerTransactionReportPipeline(Report report);

    Mono<String> triggerUserDetailsReportPipeline(Report report);
}
