package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Mono;

@RequestMapping("/idpay/selfcare")
public interface SelfcareController {
    @GetMapping("/institutions")
    Mono<InstitutionList> getInstitutions(@RequestParam(value = "taxCode") String taxCode);

}
