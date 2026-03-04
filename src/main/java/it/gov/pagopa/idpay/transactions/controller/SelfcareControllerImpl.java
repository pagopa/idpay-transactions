package it.gov.pagopa.idpay.transactions.controller;

import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class SelfcareControllerImpl implements  SelfcareController{
    @Override
    public Mono<String> getAnagrafic() {
        return null;
    }
}
