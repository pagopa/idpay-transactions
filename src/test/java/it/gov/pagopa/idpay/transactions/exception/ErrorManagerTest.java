package it.gov.pagopa.idpay.transactions.exception;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.controller.TransactionsController;
import it.gov.pagopa.idpay.transactions.dto.ErrorDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;

class ErrorManagerTest extends BaseIntegrationTest {

    @SpyBean
    TransactionsController rewardTransactionController;

    @Autowired
    WebTestClient webTestClient;

    private  Pageable defaultPageable;

    @BeforeEach
    void setUp() {
        defaultPageable = PageRequest.of(0, 2000, Sort.unsorted());
    }

    @Test
    void handleExceptionClientExceptionNoBody() {
        Mockito.when(rewardTransactionController.findAll("ClientExceptionNoBody", null, null, null, null, defaultPageable))
                .thenThrow(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, "BADREQUEST"));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionNoBody")
                        .build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody().isEmpty();
    }

    @Test
    void handleExceptionClientExceptionWithBody(){
        Mockito.when(rewardTransactionController.findAll("ClientExceptionWithBody", null, null,null,null, defaultPageable))
                .thenThrow(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "Error","Error ClientExceptionWithBody", new IllegalStateException("PROVA")));
        ErrorDTO errorClientExceptionWithBody= new ErrorDTO("Error","Error ClientExceptionWithBody");

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionWithBody")
                        .build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(errorClientExceptionWithBody);

        Mockito.when(rewardTransactionController.findAll("ClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable", null, null,null,null, defaultPageable))
                .thenThrow(new ClientExceptionWithBody(HttpStatus.BAD_REQUEST, "Error","Error ClientExceptionWithBody", new Throwable()));
        ErrorDTO errorClientExceptionWithBodyWithStatusAndTitleAndMessageAndThrowable= new ErrorDTO("Error","Error ClientExceptionWithBody");

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
        ErrorDTO expectedErrorClientException = new ErrorDTO("Error","Something gone wrong");

        Mockito.when(rewardTransactionController.findAll("ClientException", null, null,null,null, defaultPageable))
                .thenThrow(ClientException.class);
        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientException")
                        .build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);


        Mockito.when(rewardTransactionController.findAll("ClientExceptionStatusAndMessage", null, null,null,null, defaultPageable))
                .thenThrow(new ClientException(HttpStatus.BAD_REQUEST, "ClientException with httpStatus and message"));
       webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "ClientExceptionStatusAndMessage")
                        .build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorClientException);

       Mockito.when(rewardTransactionController.findAll("ClientExceptionStatusAndMessageAndThrowable", null, null,null,null, defaultPageable))
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
        ErrorDTO expectedErrorDefault = new ErrorDTO("Error","Something gone wrong");

        Mockito.when(rewardTransactionController.findAll("RuntimeException", null, null,null,null, defaultPageable))
                .thenThrow(RuntimeException.class);
        webTestClient.get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "RuntimeException")
                        .build())
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDefault);
    }
}