package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleEligibilityDecision;
import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleOperation;
import reactor.core.publisher.Mono;

public interface InvoiceLifecycleEligibilityService {

    Mono<InvoiceLifecycleEligibilityDecision> evaluate(
            String merchantId,
            String transactionId,
            InvoiceLifecycleOperation operation,
            String authorization
    );
}
