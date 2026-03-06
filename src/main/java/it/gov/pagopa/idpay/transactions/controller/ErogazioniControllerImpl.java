package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class ErogazioniControllerImpl implements ErogazioniController{
    private final ErogazioniRestClient erogazioniRestClient;

    public ErogazioniControllerImpl(ErogazioniRestClient erogazioniRestClient) {
        this.erogazioniRestClient = erogazioniRestClient;
    }

    @Override
    public Mono<Void> sendErogazione(DeliveryRequest deliveryRequest) {
        return erogazioniRestClient.sendErogazione(deliveryRequest);
    }
}
