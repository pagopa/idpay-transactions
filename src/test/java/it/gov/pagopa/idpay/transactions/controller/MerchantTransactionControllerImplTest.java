package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MerchantTransactionControllerImplTest {

    @Mock
    private MerchantTransactionService merchantTransactionService;

    @InjectMocks
    private MerchantTransactionControllerImpl controller;

    @Test
    void getMerchantTransactionsOk_noFilters() {
        MerchantTransactionsListDTO merchantTransactionsListDTO = MerchantTransactionsListDTO.builder()
                .content(List.of())
                .pageNo(0)
                .pageSize(10)
                .totalElements(0)
                .totalPages(0)
                .build();

        Pageable pageable = PageRequest.of(0, 10);

        when(merchantTransactionService.getMerchantTransactions(
                eq("testMerchant"),
                eq("INITIATIVE_ID"),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                any(Pageable.class)
        )).thenReturn(Mono.just(merchantTransactionsListDTO));

        Mono<MerchantTransactionsListDTO> resultMono = controller.getMerchantTransactions(
                "testMerchant",
                "INITIATIVE_ID",
                null,
                null,
                null,
                null,
                pageable
        );

        MerchantTransactionsListDTO result = resultMono.block();

        assertEquals(merchantTransactionsListDTO, result);

        verify(merchantTransactionService, times(1))
                .getMerchantTransactions(
                        eq("testMerchant"),
                        eq("INITIATIVE_ID"),
                        isNull(),
                        isNull(),
                        isNull(),
                        isNull(),
                        any(Pageable.class));
    }

    @Test
    void getMerchantTransactionsOk_withAllFilters() {
        MerchantTransactionsListDTO merchantTransactionsListDTO = MerchantTransactionsListDTO.builder()
                .content(List.of())
                .pageNo(0)
                .pageSize(10)
                .totalElements(0)
                .totalPages(0)
                .build();

        Pageable pageable = PageRequest.of(0, 10);

        when(merchantTransactionService.getMerchantTransactions(
                eq("testMerchant"),
                eq("INITIATIVE_ID"),
                eq("FISCALCODE1"),
                eq("REWARDED"),
                eq("BATCH_1"),
                eq("INV_OK"),
                any(Pageable.class)
        )).thenReturn(Mono.just(merchantTransactionsListDTO));

        Mono<MerchantTransactionsListDTO> resultMono = controller.getMerchantTransactions(
                "testMerchant",
                "INITIATIVE_ID",
                "FISCALCODE1",
                "REWARDED",
                "BATCH_1",
                "INV_OK",
                pageable
        );

        MerchantTransactionsListDTO result = resultMono.block();

        assertEquals(merchantTransactionsListDTO, result);

        verify(merchantTransactionService, times(1))
                .getMerchantTransactions(
                        eq("testMerchant"),
                        eq("INITIATIVE_ID"),
                        eq("FISCALCODE1"),
                        eq("REWARDED"),
                        eq("BATCH_1"),
                        eq("INV_OK"),
                        any(Pageable.class));
    }
}
