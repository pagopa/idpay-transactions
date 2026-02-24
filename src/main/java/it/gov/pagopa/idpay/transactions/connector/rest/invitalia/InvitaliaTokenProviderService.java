package it.gov.pagopa.idpay.transactions.connector.rest.invitalia;

import reactor.core.publisher.Mono;

public interface InvitaliaTokenProviderService {
    Mono<String> retrieveToken();
}
