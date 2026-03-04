package it.gov.pagopa.idpay.transactions.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/selfcare")
public interface SelfcareController {
    @GetMapping("/anagrafic")
    Mono<String> getAnagrafic();

}
