package it.gov.pagopa.idpay.transactions.connector.rest.initiative;

import it.gov.pagopa.idpay.transactions.connector.rest.dto.InitiativeData;
import reactor.core.publisher.Mono;

public interface InitiativeRestClient {
    Mono<InitiativeData> getInitiative(String authorization, String organizationId, String initiativeId);
}
