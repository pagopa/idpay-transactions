package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.config.ServiceExceptionConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.dto.AnagraficaDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import it.gov.pagopa.idpay.transactions.dto.ErogazioneDTO;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = InvitaliaControllerImpl.class)
@Import({ServiceExceptionConfig.class})
class InvitaliaControllerImplTest {
    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private InvitaliaTokenProviderService invitaliaTokenProviderService;
    @MockitoBean
    private ErogazioniRestClient erogazioniRestClient;

    @Test
    void getToken_Success() {
        String token = "TOKEN";

        when(invitaliaTokenProviderService.retrieveToken()).thenReturn(Mono.just(token));

        webTestClient.get()
                .uri("/idpay/invitalia/token")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .isEqualTo(token);

        verify(invitaliaTokenProviderService).retrieveToken();
    }

    @Test
    void getToken_Error() {

        when(invitaliaTokenProviderService.retrieveToken()).thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        webTestClient.get()
                .uri("/idpay/invitalia/token")
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(String.class);

        verify(invitaliaTokenProviderService).retrieveToken();
    }



    @Test
    void postErogazione_Success() {
        DeliveryRequest request = DeliveryRequest.builder()
                .id("BATCH_ID")
                .anagrafica(AnagraficaDTO.builder()
                        .partitaIvaCliente("12345678901")
                        .codiceFiscaleCliente("RSSMRA80A01H501W")
                        .ragioneSocialeIntestatario("Ragione Sociale")
                        .pec("test@pec.it")
                        .indirizzo("Via Roma 1")
                        .cap("00100")
                        .localita("Roma")
                        .provincia("RM")
                        .build())
                .erogazione(ErogazioneDTO.builder()
                        .idPratica("PRATICA_ID")
                        .importo(10.0)
                        .dataAmmissione(LocalDateTime.now())
                        .ibanBeneficiario("IT12X0123401234000000123456")
                        .intestatarioContoCorrente("Mario Rossi")
                        .autorizzatore("Gianluca Fiorillo")
                        .build())
                .build();

        DeliveryOutcomeDTO expectedOutcome = DeliveryOutcomeDTO.builder()
                .succeded(true)
                .message("Erogazione inserita con successo")
                .idRichiesta("BATCH_ID")
                .build();

        when(erogazioniRestClient.postErogazione(any(DeliveryRequest.class)))
                .thenReturn(Mono.just(expectedOutcome));

        webTestClient.post()
                .uri("/idpay/invitalia/erogazioni")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.succeded").isEqualTo(true)
                .jsonPath("$.message").isEqualTo("Erogazione inserita con successo");

        verify(erogazioniRestClient).postErogazione(any(DeliveryRequest.class));
    }
    @Test
    void postErogazione_InternalServerError() {
        DeliveryRequest request = DeliveryRequest.builder().id("BATCH_ID").build();

        when(erogazioniRestClient.postErogazione(any(DeliveryRequest.class)))
                .thenReturn(Mono.error(new RuntimeException("API Error")));

        webTestClient.post()
                .uri("/idpay/invitalia/erogazioni")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().is5xxServerError();
    }
}
