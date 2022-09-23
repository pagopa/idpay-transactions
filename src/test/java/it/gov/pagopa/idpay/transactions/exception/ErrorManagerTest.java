package it.gov.pagopa.idpay.transactions.exception;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.controller.TransactionsController;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
@AutoConfigureWebTestClient
class ErrorManagerTest extends BaseIntegrationTest {

    @SpyBean
    TransactionsController rewardTransactionController;

    @Autowired
    WebTestClient webTestClient;

    //TODO fix test

    @Test
    void handleExceptionClientExceptionNoBody() {
        Mockito.when(rewardTransactionController.findAll(Mockito.eq("ClientExceptionNoBody"), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenThrow(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionNoBody")
                        .build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody().isEmpty();
    }
/*
    @Test
    void handleExceptionClientExceptionWithBody(){
        Mockito.when(rewardTransactionController.findAll("ClientExceptionWithBody", null, null,null,null))
                .thenThrow(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "Error","Error ClientExceptionWithBody"));
        ErrorDTO errorClientExceptionWithBody= new ErrorDTO(Severity.ERROR,"Error","Error ClientExceptionWithBody");

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionWithBody")
                        .build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(errorClientExceptionWithBody);

        Mockito.when(rewardTransactionController.findAll("ClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable", null, null,null,null))
                .thenThrow(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "Error","Error ClientExceptionWithBody", new Throwable()));
        ErrorDTO errorClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable= new ErrorDTO(Severity.ERROR,"Error","Error ClientExceptionWithBody");

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable")
                        .build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(errorClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable);
    }

    @Test
    void handleExceptionClientExceptionTest(){
        ErrorDTO expectedErrorClientException = new ErrorDTO(Severity.ERROR,"Error","Something gone wrong");

        Mockito.when(rewardTransactionController.findAll("ClientException", null, null,null,null))
                .thenThrow(ClientException.class);
        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientException")
                        .build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);


        Mockito.when(rewardTransactionController.findAll("ClientExceptionStatusAndMessage", null, null,null,null))
                .thenThrow(new ClientException(HttpStatus.BAD_REQUEST, "ClientException with httpStatus and message"));
       webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionStatusAndMessage")
                        .build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);

       Mockito.when(rewardTransactionController.findAll("ClientExceptionStatusAndMessageAndThrowable", null, null,null,null))
                .thenThrow(new ClientException(HttpStatus.BAD_REQUEST, "ClientException with httpStatus, message and throwable", new Throwable()));
       webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionStatusAndMessageAndThrowable")
                        .build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);
    }

    @Test
    void handleExceptionRuntimeException(){
        ErrorDTO expectedErrorDefault = new ErrorDTO(Severity.ERROR,"Error","Something gone wrong");

        Mockito.when(rewardTransactionController.findAll("RuntimeException", null, null,null,null))
                .thenThrow(RuntimeException.class);
        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "RuntimeException")
                        .build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDefault);
    }

 */
}