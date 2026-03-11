package it.gov.pagopa.idpay.transactions.connector.rest.selfcare;

import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import reactor.core.publisher.Mono;

public interface SelfcareInstitutionsRestClient {
    Mono<InstitutionList> getInstitutions (String merchantFiscalCode);
}
