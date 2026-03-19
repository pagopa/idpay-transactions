package it.gov.pagopa.idpay.transactions.connector.rest.erogazioni;


import com.github.javafaker.Faker;
import it.gov.pagopa.common.config.JsonConfig;
import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import it.gov.pagopa.common.reactive.rest.config.WebClientConfig;
import it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest;
import reactor.test.StepVerifier;

import static it.gov.pagopa.common.reactive.wireMock.BaseWireMockTest.WIREMOCK_TEST_PROP2BASEPATH_MAP_PREFIX;
import static org.junit.jupiter.api.Assertions.*;


@ContextConfiguration(
        classes = {
                ErogazioniRestClientImpl.class,
                WebClientConfig.class,
                JsonConfig.class
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

    @Test
    void postErogazione_truncateRegioneSociale_greaterThan140_ok() {
        String batchId = "BATCH_OK";
        DeliveryRequest request = createRequest(batchId, "12345678901");
        Faker faker = new Faker();
        String lorem = faker.lorem().characters(200);
        request.getAnagrafica().setRagioneSocialeIntestatario(lorem);

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        erogazioniRestClient.postErogazione(request).block();

        assertEquals(140, request.getAnagrafica().getRagioneSocialeIntestatario().length());
    }

    @Test
    void postErogazione_truncateRegioneSociale_lessThan140_ok() {
        String batchId = "BATCH_OK";
        DeliveryRequest request = createRequest(batchId, "12345678901");
        Faker faker = new Faker();
        String lorem = faker.lorem().characters(120);
        request.getAnagrafica().setRagioneSocialeIntestatario(lorem);

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        erogazioniRestClient.postErogazione(request).block();

        assertEquals(lorem, request.getAnagrafica().getRagioneSocialeIntestatario());
    }

    @Test
    void postErogazione_truncateRegioneSociale_null_ok() {
        String batchId = "BATCH_OK";
        DeliveryRequest request = createRequest(batchId, "12345678901");
        request.getAnagrafica().setRagioneSocialeIntestatario(null);

        Mockito.when(invitaliaTokenProviderService.retrieveToken())
                .thenReturn(Mono.just("MOCK_TOKEN"));

        erogazioniRestClient.postErogazione(request).block();

        assertNull(request.getAnagrafica().getRagioneSocialeIntestatario());
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