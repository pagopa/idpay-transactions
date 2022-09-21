package it.gov.pagopa.idpay.transactions.exception;

import it.gov.pagopa.idpay.transactions.BaseIntegrationTest;
import it.gov.pagopa.idpay.transactions.controller.TransactionsController;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.SpyBean;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

class ErrorManagerTest extends BaseIntegrationTest {

    @SpyBean
    TransactionsController rewardTransactionController;

    @Test
    void handleException() {
        LocalDateTime startDate = LocalDateTime.now();
        LocalDateTime endDate = startDate.plusDays(1L);
        BigDecimal amount = new BigDecimal("30.00");
        Mockito.when(rewardTransactionController.findAll("ClientExceptionNoBody", "userId", startDate,endDate,amount))
                .thenThrow(ClientExceptionNoBody.class);

        Flux<RewardTransaction> ExceptionNoBodyFlux = Flux.defer(() -> rewardTransactionController.findAll("ClientExceptionNoBody", "userId", null,null,amount))
                .onErrorResume(e -> {
                    Assertions.assertTrue(e instanceof ClientExceptionNoBody);
                    return Flux.empty();
                });
        Assertions.assertEquals(0, ExceptionNoBodyFlux.count().block());

        Mockito.when(rewardTransactionController.findAll("ClientException", "userId", null,null,amount))
                .thenThrow(ClientException.class);
        Flux<RewardTransaction> ClientExceptionFlux = Flux.defer(() -> rewardTransactionController.findAll("ClientException", "userId", null,null,amount))
                .onErrorResume(e -> {
                    Assertions.assertTrue(e instanceof ClientException);
                    return Flux.empty();
                });
        Assertions.assertEquals(0, ClientExceptionFlux.count().block());

    }
}