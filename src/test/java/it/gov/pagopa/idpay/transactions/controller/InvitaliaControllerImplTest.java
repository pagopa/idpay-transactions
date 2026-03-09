package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.config.ServiceExceptionConfig;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.InvitaliaTokenProviderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = InvitaliaControllerImpl.class)
@Import({ServiceExceptionConfig.class})
class InvitaliaControllerImplTest {
    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private InvitaliaTokenProviderService invitaliaTokenProviderService;

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
}