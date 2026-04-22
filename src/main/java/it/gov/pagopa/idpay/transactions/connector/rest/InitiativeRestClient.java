package it.gov.pagopa.idpay.transactions.connector.rest;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeDetailDTO;
import reactor.core.publisher.Mono;

public interface InitiativeRestClient {

    Mono<InitiativeDetailDTO> getInitiativeBeneficiaryDetail(String initiativeId);

}
