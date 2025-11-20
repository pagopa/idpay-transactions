package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.*;

@WebFluxTest(controllers = MerchantTransactionControllerImpl.class)
class MerchantTransactionControllerImplTest {

    @Mock
    MerchantTransactionService merchantTransactionService;

    @Autowired
    protected WebTestClient webClient;

    @Test
    void getMerchantTransactionsOk_noFilters() {
        MerchantTransactionsListDTO merchantTransactionsListDTO = MerchantTransactionsListDTO.builder()
                .content(java.util.List.of())
                .pageNo(0)
                .pageSize(10)
                .totalElements(0)
                .totalPages(0)
                .build();

        Mockito.when(merchantTransactionService.getMerchantTransactions(
                        eq("test"),
                        eq("INITIATIVE_ID"),
                        isNull(),  // fiscalCode
                        isNull(),  // status
                        isNull(),  // rewardBatchId
                        isNull(),  // invStatus
                        any()      // pageable
                ))
                .thenReturn(Mono.just(merchantTransactionsListDTO));

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed")
                        .build("INITIATIVE_ID"))
                .header("x-merchant-id", "test")
                .exchange()
                .expectStatus().isOk()
                .expectBody(MerchantTransactionsListDTO.class)
                .isEqualTo(merchantTransactionsListDTO);

        Mockito.verify(merchantTransactionService, Mockito.times(1))
                .getMerchantTransactions(
                        eq("test"),
                        eq("INITIATIVE_ID"),
                        isNull(),
                        isNull(),
                        isNull(),
                        isNull(),
                        any());
    }

    @Test
    void getMerchantTransactionsOk_withAllFilters() {
        MerchantTransactionsListDTO merchantTransactionsListDTO = MerchantTransactionsListDTO.builder()
                .content(java.util.List.of())
                .pageNo(0)
                .pageSize(10)
                .totalElements(0)
                .totalPages(0)
                .build();

        Mockito.when(merchantTransactionService.getMerchantTransactions(
                        eq("test"),
                        eq("INITIATIVE_ID"),
                        eq("FISCALCODE1"),
                        eq("REWARDED"),
                        eq("BATCH_1"),
                        eq("INV_OK"),
                        any()
                ))
                .thenReturn(Mono.just(merchantTransactionsListDTO));

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed")
                        .queryParam("fiscalCode", "FISCALCODE1")
                        .queryParam("status", "REWARDED")
                        .queryParam("rewardBatchId", "BATCH_1")
                        .queryParam("invStatus", "INV_OK")
                        .build("INITIATIVE_ID"))
                .header("x-merchant-id", "test")
                .exchange()
                .expectStatus().isOk()
                .expectBody(MerchantTransactionsListDTO.class)
                .isEqualTo(merchantTransactionsListDTO);

        Mockito.verify(merchantTransactionService, Mockito.times(1))
                .getMerchantTransactions(
                        eq("test"),
                        eq("INITIATIVE_ID"),
                        eq("FISCALCODE1"),
                        eq("REWARDED"),
                        eq("BATCH_1"),
                        eq("INV_OK"),
                        any());
    }
}
