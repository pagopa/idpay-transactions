package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.config.ServiceExceptionConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = SelfcareControllerImpl.class)
@Import({ServiceExceptionConfig.class})
class SelfcareControllerImplTest {
    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private SelfcareInstitutionsRestClient selfcareInstitutionsRestClient;

    @Test
    void getInstitution_Success() {
        String taxCode = "TAX_CODE";

        InstitutionDTO institution = new InstitutionDTO();
        InstitutionList institutionList = new InstitutionList();
        institutionList.setInstitutions(List.of(institution));

        when(selfcareInstitutionsRestClient.getInstitutions(taxCode)).thenReturn(Mono.just(institutionList));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/selfcare/institutions")
                        .queryParam("taxCode", taxCode)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody(InstitutionList.class)
                .isEqualTo(institutionList);

        verify(selfcareInstitutionsRestClient).getInstitutions(anyString());
    }

    @Test
    void getToken_Error() {
        String taxCode = "TAX_CODE";

        when(selfcareInstitutionsRestClient.getInstitutions(taxCode)).thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/selfcare/institutions")
                        .queryParam("taxCode", taxCode)
                        .build())
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(String.class);

        verify(selfcareInstitutionsRestClient).getInstitutions(anyString());
    }
}