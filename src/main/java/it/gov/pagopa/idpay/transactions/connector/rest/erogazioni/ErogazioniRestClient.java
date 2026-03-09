package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;

import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import reactor.core.publisher.Mono;

public interface ErogazioniRestClient {
    Mono<Void> postErogazione(DeliveryRequest deliveryRequest);
}
