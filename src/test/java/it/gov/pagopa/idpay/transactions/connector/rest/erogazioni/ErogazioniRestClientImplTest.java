package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;


import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import it.gov.pagopa.idpay.transactions.dto.DeliveryRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
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
                "app.erogazioni.authorizer=TEST_AUTH",
                "app.erogazioni.retry.max-attempts=2",
                "app.erogazioni.retry.delay-millis=100"
        }
)
class ErogazioniRestClientImplTest extends BaseWireMockTest {

    @Autowired
    private ErogazioniRestClient erogazioniRestClient;

    @MockBean
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
                .expectErrorMatches(ex -> ex instanceof RuntimeException &&
                        ex.getMessage().contains("Error sending erogazione after retry"))
                .verify();
    }

    @Test
    void postErogazione_formatPiva_ok() {
        String batchId = "BATCH_CF";
        DeliveryRequest request = createRequest(batchId, "RSSMRA80A01H501W");

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        erogazioniRestClient.postErogazione(request).block();

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

    @Test
    void getOutcome_ok_returnsCompleted() {
        String batchId = "BATCH_123";

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        StepVerifier.create(erogazioniRestClient.getOutcome(batchId))
                .assertNext(outcome -> {
                    assertEquals("COMPLETATO", outcome.getErogazione().getStatus());

                    BigDecimal expectedAmount = new BigDecimal("1000.0");
                    BigDecimal actualAmount = outcome.getErogazione().getAmountPaid() != null
                            ? new BigDecimal(outcome.getErogazione().getAmountPaid().toString())
                            : null;
                    assertNotNull(actualAmount);
                    assertEquals(0, expectedAmount.compareTo(actualAmount), "Amount mismatch");

                    assertEquals(LocalDate.of(2026, 3, 10), outcome.getErogazione().getDateValue());
                    assertEquals("OK", outcome.getMessage());
                })
                .verifyComplete();
    }

    @Test
    void getOutcome_ko_throwsError() {
        String batchId = "BATCH_456";

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN_KO"));

        StepVerifier.create(erogazioniRestClient.getOutcome(batchId))
                .expectErrorMatches(RuntimeException.class::isInstance)
                .verify();
    }
}