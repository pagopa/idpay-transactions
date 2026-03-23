package it.gov.pagopa.idpay.transactions.connector.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;

class UserRestClientImplIntegretedTest {

    private ExchangeFunction exchangeFunction;
    private WebClient webClient;
    private UserRestClientImpl client;

    @BeforeEach
    void setUp() {
        exchangeFunction = Mockito.mock(ExchangeFunction.class);

        webClient = WebClient.builder()
                .exchangeFunction(exchangeFunction)
                .baseUrl("http://fake-url")
                .defaultHeader("x-api-key", "fake-key")
                .build();

        client = new UserRestClientImpl(
                "http://fake-url",
                "fake-key",
                10,
                1,
                WebClient.builder().exchangeFunction(exchangeFunction)
        );
    }

    @Test
    void retrieveUserInfo_ok() {
        ClientResponse clientResponse = ClientResponse
                .create(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("{ }")
                .build();

        Mockito.when(exchangeFunction.exchange(any()))
                .thenReturn(Mono.just(clientResponse));

        StepVerifier.create(client.retrieveUserInfo("token123"))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void retrieveUserInfo_notFound() {
        ClientResponse clientResponse = ClientResponse
                .create(HttpStatus.NOT_FOUND)
                .build();

        Mockito.when(exchangeFunction.exchange(any()))
                .thenReturn(Mono.just(clientResponse));

        StepVerifier.create(client.retrieveUserInfo("token123"))
                .verifyComplete(); // Mono.empty()
    }

    @Test
    void retrieveUserInfo_badRequest() {
        ClientResponse clientResponse = ClientResponse
                .create(HttpStatus.BAD_REQUEST)
                .build();

        Mockito.when(exchangeFunction.exchange(any()))
                .thenReturn(Mono.just(clientResponse));

        StepVerifier.create(client.retrieveUserInfo("invalid"))
                .verifyComplete(); // Mono.empty()
    }

    // =========================
    // retrieveFiscalCodeInfo
    // =========================

    @Test
    void retrieveFiscalCodeInfo_ok() {
        ClientResponse clientResponse = ClientResponse
                .create(HttpStatus.OK)
                .header("Content-Type", "application/json")
                .body("{ }")
                .build();

        Mockito.when(exchangeFunction.exchange(any()))
                .thenReturn(Mono.just(clientResponse));

        StepVerifier.create(client.retrieveFiscalCodeInfo("RSSMRA80A01H501U"))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void retrieveFiscalCodeInfo_notFound() {
        ClientResponse clientResponse = ClientResponse
                .create(HttpStatus.NOT_FOUND)
                .build();

        Mockito.when(exchangeFunction.exchange(any()))
                .thenReturn(Mono.just(clientResponse));

        StepVerifier.create(client.retrieveFiscalCodeInfo("XXX"))
                .verifyComplete();
    }

    @Test
    void retrieveFiscalCodeInfo_badRequest() {
        ClientResponse clientResponse = ClientResponse
                .create(HttpStatus.BAD_REQUEST)
                .build();

        Mockito.when(exchangeFunction.exchange(any()))
                .thenReturn(Mono.just(clientResponse));

        StepVerifier.create(client.retrieveFiscalCodeInfo("INVALID"))
                .verifyComplete();
    }
}