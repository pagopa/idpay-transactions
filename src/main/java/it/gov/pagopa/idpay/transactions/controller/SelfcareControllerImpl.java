package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class SelfcareControllerImpl implements  SelfcareController{
    private final SelfcareInstitutionsRestClient selfcareInstitutionsRestClient;

    public SelfcareControllerImpl(SelfcareInstitutionsRestClient selfcareInstitutionsRestClient) {
        this.selfcareInstitutionsRestClient = selfcareInstitutionsRestClient;
    }

    @Override
    public Mono<InstitutionList> getInstitutions(String taxCode) {
        return selfcareInstitutionsRestClient.getInstitutions(taxCode);
    }
}
