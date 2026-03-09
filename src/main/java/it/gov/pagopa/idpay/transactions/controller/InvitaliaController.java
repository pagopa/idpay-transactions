package it.gov.pagopa.idpay.transactions.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/invitalia")
public interface InvitaliaController {
    @GetMapping("/token")
    Mono<String> getToken();
}
