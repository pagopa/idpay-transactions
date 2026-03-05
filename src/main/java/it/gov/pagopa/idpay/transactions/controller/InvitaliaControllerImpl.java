package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class InvitaliaControllerImpl implements InvitaliaController{
    private final InvitaliaTokenProviderService invitaliaTokenProviderService;

    public InvitaliaControllerImpl(InvitaliaTokenProviderService invitaliaTokenProviderService) {
        this.invitaliaTokenProviderService = invitaliaTokenProviderService;
    }

    @Override
    public Mono<String> getToken() {
        return invitaliaTokenProviderService.retrieveToken();
    }
}
