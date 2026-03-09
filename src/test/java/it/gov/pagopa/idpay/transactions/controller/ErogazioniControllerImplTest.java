package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.config.ServiceExceptionConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClientImpl;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.SelfcareInstitutionsRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto.InstitutionList;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = ErogazioniControllerImpl.class)
@Import({ServiceExceptionConfig.class})
class ErogazioniControllerImplTest {
    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private ErogazioniRestClient erogazioniRestClient;

    @Test
    void sendErogazione_Success() {
        // Given
        DeliveryRequest request = DeliveryRequest.builder()
                .id("BATCH_ID")
                .idPratica("PRATICA_ID")
                .importo(1000L)
                .dataAmmissione(LocalDateTime.now())
                .ragioneSocialeIntestatario("Ragione Sociale")
                .partitaIvaCliente("12345678901")
                .codiceFiscaleCliente("RSSMRA80A01H501W")
                .ibanBeneficiario("IT12X0123401234000000123456")
                .intestatarioContoCorrente("Mario Rossi")
                .pec("test@pec.it")
                .indirizzo("Via Roma 1")
                .cap("00100")
                .localita("Roma")
                .provincia("RM")
                .build();

        // Configuriamo il mock per restituire Mono.empty() (corrisponde a Mono<Void>)
        when(erogazioniRestClient.sendErogazione(any(DeliveryRequest.class)))
                .thenReturn(Mono.empty());

        // When & Then
        webTestClient.post()
                .uri("/idpay/erogazioni/sendErogazione")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody().isEmpty();

        // Verifichiamo che il restClient sia stato effettivamente chiamato
        verify(erogazioniRestClient).sendErogazione(any(DeliveryRequest.class));
    }

    @Test
    void sendErogazione_InternalServerError() {
        // Given
        DeliveryRequest request = DeliveryRequest.builder().id("BATCH_ID").build();

        // Simuliamo un errore dal client
        when(erogazioniRestClient.sendErogazione(any(DeliveryRequest.class)))
                .thenReturn(Mono.error(new RuntimeException("API Error")));

        // When & Then
        webTestClient.post()
                .uri("/idpay/erogazioni/sendErogazione")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().is5xxServerError();
    }
}