package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class InvitaliaControllerImpl implements InvitaliaController{
    private final InvitaliaTokenProviderService invitaliaTokenProviderService;
    private final ErogazioniRestClient erogazioniRestClient;

    public InvitaliaControllerImpl(InvitaliaTokenProviderService invitaliaTokenProviderService, ErogazioniRestClient erogazioniRestClient) {
        this.invitaliaTokenProviderService = invitaliaTokenProviderService;
        this.erogazioniRestClient = erogazioniRestClient;
    }

    @Override
    public Mono<String> getToken() {
        return invitaliaTokenProviderService.retrieveToken();
    }

    @Override
    public Mono<DeliveryOutcomeDTO> postErogazione(DeliveryRequest deliveryRequest) {
        return erogazioniRestClient.postErogazione(deliveryRequest);
    }

    @Override
    public Mono<InvitaliaOutcomeResponseDTO> checkRefundOutcome(String rewardBatchId) {
        return erogazioniRestClient.getOutcome(rewardBatchId);
    }
}