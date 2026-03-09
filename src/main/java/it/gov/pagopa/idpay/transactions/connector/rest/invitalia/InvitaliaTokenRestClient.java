package it.gov.pagopa.idpay.transactions.connector.rest.invitalia;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.TokenDTO;
import reactor.core.publisher.Mono;

public interface InvitaliaTokenRestClient {
    Mono<TokenDTO> getToken();
}
