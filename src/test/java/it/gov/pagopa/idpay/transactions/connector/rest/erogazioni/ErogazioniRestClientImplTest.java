package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;


import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import reactor.test.StepVerifier;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;


@ContextConfiguration(
        classes = {
                ErogazioniRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.erogazioni.erogazioni-url",
                "app.erogazioni.autorizzatore=TEST_AUTH",
                "app.erogazioni.retry.max-attempts=2",
                "app.erogazioni.retry.delay-millis=100"
        }
)
class ErogazioniRestClientImplTest extends BaseWireMockTest {

    @Autowired
    private ErogazioniRestClient erogazioniRestClient;

    @MockBean
    private InvitaliaTokenProviderService invitaliaTokenProviderService;

    private static final String URI_EROGAZIONI = "/erogazioni";

    @Test
    void sendErogazione_ok() {
        // Given
        String batchId = "BATCH_OK";
        DeliveryRequest request = createRequest(batchId, "12345678901");

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        // Definiamo lo stub per l'url relativo /erogazioni
        // WireMock risponderà sulla porta 65099 (o quella dinamica scelta)
        stubFor(post(urlEqualTo(URI_EROGAZIONI))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")));

        // When
        // Se qui esplode ancora sulla 8080, controlla che erogazioniRestClient
        // abbia ricevuto l'URL giusto nel costruttore (fai un debug o un log)
        StepVerifier.create(erogazioniRestClient.sendErogazione(request))
                .verifyComplete();
    }

    @Test
    void sendErogazione_koInternalServerError() {
        // Given
        String batchId = "BATCH_KO";
        DeliveryRequest request = createRequest(batchId, "12345678901");

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        // Simula errore 500
        stubFor(post(urlEqualTo("/erogazioni"))
                .willReturn(aResponse().withStatus(500)));

        // When & Then
        StepVerifier.create(erogazioniRestClient.sendErogazione(request))
                .expectErrorMatches(ex -> ex instanceof RuntimeException &&
                        ex.getMessage().contains("Error sending erogazione after retry"))
                .verify();
    }

    @Test
    void sendErogazione_formatPiva_ok() {
        // Given
        String batchId = "BATCH_CF";
        // Codice fiscale da 16 caratteri
        DeliveryRequest request = createRequest(batchId, "RSSMRA80A01H501W");

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        // When
        erogazioniRestClient.sendErogazione(request).block();

        // Then
        // Verifichiamo che la logica interna abbia trasformato la P.IVA in 11 zeri
        assertEquals("00000000000", request.getPartitaIvaCliente());
        assertEquals("TEST_AUTH", request.getAutorizzatore());
    }


    private DeliveryRequest createRequest(String id, String piva) {
        return DeliveryRequest.builder()
                .id(id)
                .idPratica(id)
                .partitaIvaCliente(piva)
                .importo(1000L)
                .dataAmmissione(LocalDateTime.now())
                .build();
    }
}