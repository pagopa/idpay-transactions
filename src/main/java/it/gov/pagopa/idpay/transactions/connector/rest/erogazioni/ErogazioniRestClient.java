package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import reactor.core.publisher.Mono;

public interface ErogazioniRestClient {
    Mono<InvitaliaOutcomeResponseDTO> getOutcome(String requestId);
    Mono<DeliveryOutcomeDTO> postErogazione(DeliveryRequest deliveryRequest);
}
