package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;


import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.dto.AnagraficaDTO;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import it.gov.pagopa.idpay.transactions.dto.ErogazioneDTO;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import reactor.test.StepVerifier;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;
import static org.junit.jupiter.api.Assertions.*;


@ContextConfiguration(
        classes = {
                ErogazioniRestClientImpl.class,
                WebClientConfig.class
        })
@TestPropertySource(
        properties = {
                WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX + "app.erogazioni.erogazioni-url",
                "app.erogazioni.authorizer=TEST_AUTH",
                "app.erogazioni.retry.max-attempts=2",
                "app.erogazioni.retry.delay-millis=100"
        }
)
class ErogazioniRestClientImplTest extends BaseWireMockTest {

    @Autowired
    private ErogazioniRestClient erogazioniRestClient;

    @MockitoBean
    private InvitaliaTokenProviderService invitaliaTokenProviderService;


    @Test
    void postErogazione_ok() {
        String batchId = "BATCH_OK";
        DeliveryRequest request = createRequest(batchId, "12345678901");

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        StepVerifier.create(erogazioniRestClient.postErogazione(request))
                .verifyComplete();
    }

    @Test
    void postErogazione_koInternalServerError() {
        String batchId = "BATCH_KO";
        DeliveryRequest request = createRequest(batchId, "12345678901");

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN_KO"));

        StepVerifier.create(erogazioniRestClient.postErogazione(request))
                .assertNext(outcome -> {
                    assertFalse(outcome.isSucceded());
                    assertTrue(outcome.getMessage().contains("Technical error"));
                    assertTrue(outcome.getMessage().contains("Server error during erogazione"));
                    assertNotNull(outcome.getTimestamp());
                })
                .verifyComplete();
    }

    @Test
    void postErogazione_formatPiva_ok() {
        String batchId = "BATCH_CF";
        DeliveryRequest request = createRequest(batchId, "RSSMRA80A01H501W");

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));


        erogazioniRestClient.postErogazione(request).block();

        assertEquals("00000000000", request.getAnagrafica().getPartitaIvaCliente());
        assertEquals("TEST_AUTH", request.getErogazione().getAutorizzatore());
    }

    private DeliveryRequest createRequest(String id, String piva) {
        return DeliveryRequest.builder()
                .id(id)
                .anagrafica(AnagraficaDTO.builder()
                        .partitaIvaCliente(piva)
                        .build())
                .erogazione(ErogazioneDTO.builder()
                        .idPratica(id)
                        .importo(10.0)
                        .dataAmmissione(LocalDateTime.now())
                        .build())
                .build();
    }
}