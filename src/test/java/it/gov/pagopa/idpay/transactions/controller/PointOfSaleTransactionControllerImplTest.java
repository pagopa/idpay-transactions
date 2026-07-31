package it.gov.pagopa.idpay.transactions.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockUser;

import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.cache.CacheManager;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@WebFluxTest(controllers = PointOfSaleTransactionControllerImpl.class)
class PointOfSaleTransactionControllerImplTest {

    @Autowired private WebTestClient webClient;
    @MockitoBean private PointOfSaleTransactionService service;
    @MockitoBean private PointOfSaleTransactionMapper mapper;
    @MockitoBean private CacheManager cacheManager;

    @Test
    void retainedPointOfSaleSearchRouteDelegatesToService() {
        RewardTransaction transaction = RewardTransaction.builder().id("transaction").build();
        when(service.getPointOfSaleTransactions(
                eq("merchant"), eq("initiative"), eq("pos"), eq(null), any(TrxFiltersDTO.class), any()))
                .thenReturn(Mono.just(new PageImpl<>(List.of(transaction), PageRequest.of(0, 20), 1)));
        when(mapper.toDTO(eq(transaction), eq("initiative"), eq(null))).thenReturn(Mono.empty());

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/idpay/initiatives/initiative/point-of-sales/pos/transactions/processed")
                .header("x-merchant-id", "merchant")
                .header("x-point-of-sale-id", "pos")
                .exchange()
                .expectStatus().isOk();

        verify(service).getPointOfSaleTransactions(
                eq("merchant"), eq("initiative"), eq("pos"), eq(null), any(TrxFiltersDTO.class), any());
    }

    @Test
    void retainedInvoiceDownloadRouteDelegatesToService() {
        when(service.downloadTransactionInvoice("merchant", "pos", "transaction"))
                .thenReturn(Mono.just(DownloadInvoiceResponseDTO.builder().invoiceUrl("signed-url").build()));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/idpay/pos/transactions/transaction/download")
                .header("x-merchant-id", "merchant")
                .exchange()
                .expectStatus().isOk();

        verify(service).downloadTransactionInvoice("merchant", "pos", "transaction");
    }

    @Test
    void retainedBatchPointOfSaleReadRouteDelegatesToService() {
        when(service.getDistinctFranchiseAndPosByRewardBatchId("batch", "merchant"))
                .thenReturn(Mono.just(List.of(new FranchisePointOfSaleDTO())));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/idpay/point-of-sales/batch")
                .header("x-merchant-id", "merchant")
                .exchange()
                .expectStatus().isOk();

        verify(service).getDistinctFranchiseAndPosByRewardBatchId("batch", "merchant");
    }

    @Test
    void retainedPointOfSaleRoutesRejectTokenAndPathPointOfSaleMismatches() {
        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/idpay/initiatives/initiative/point-of-sales/pos/transactions/processed")
                .header("x-merchant-id", "merchant")
                .header("x-point-of-sale-id", "other-pos")
                .exchange()
                .expectStatus().isForbidden();

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/idpay/pos/transactions/transaction/download")
                .header("x-merchant-id", "merchant")
                .header("x-point-of-sale-id", "other-pos")
                .exchange()
                .expectStatus().isForbidden();
    }
}
