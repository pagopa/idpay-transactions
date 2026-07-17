package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockUser;

@WebFluxTest(controllers = {TransactionsController.class})
class TransactionsControllerImplTest {
    @MockitoBean
    RewardTransactionService rewardTransactionService;

    @Autowired
    protected WebTestClient webClient;
    @MockitoBean
    CacheManager cacheManager;


    @Test
    void cleanupInvoicedTransactions_defaultChunkSize() {
        Mockito.when(rewardTransactionService.assignInvoicedTransactionsToBatches(Mockito.anyInt(),
                        Mockito.anyInt(), Mockito.anyBoolean(), Mockito.isNull()))
                .thenReturn(Mono.empty());

        webClient.mutateWith(mockUser()).mutateWith(csrf()).post()
                .uri("/idpay/transactions/cleanup")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(String.class);

        Mockito.verify(rewardTransactionService, Mockito.times(1))
                .assignInvoicedTransactionsToBatches(
                        Mockito.eq(200),
                        Mockito.eq(1),
                        Mockito.eq(false),
                        Mockito.isNull());
    }

    @Test
    void cleanupInvoicedTransactions_customChunkSize() {
        Mockito.when(rewardTransactionService.assignInvoicedTransactionsToBatches(Mockito.anyInt(),
                        Mockito.anyInt(), Mockito.anyBoolean(), Mockito.isNull()))
                .thenReturn(Mono.empty());

        int customChunkSize = 500;
        int customIteration = 10;

        webClient.mutateWith(mockUser()).mutateWith(csrf()).post()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions/cleanup")
                        .queryParam("chunkSize", customChunkSize)
                        .queryParam("repetitionsNumber", customIteration)
                        .build())
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(String.class);

        Mockito.verify(rewardTransactionService, Mockito.times(1))
                .assignInvoicedTransactionsToBatches(
                        Mockito.eq(customChunkSize),
                        Mockito.eq(customIteration), Mockito.eq(false),
                        Mockito.isNull()
                );
    }
}