package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import org.junit.jupiter.api.BeforeEach;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MerchantTransactionControllerImplTest {

    @Mock
    private MerchantTransactionService merchantTransactionService;

    @InjectMocks
    private MerchantTransactionControllerImpl merchantTransactionController;

    private Pageable paging;

    @BeforeEach
    void setUp() {
        paging = PageRequest.of(0, 10);
    }

    @Test
    void findMerchantTransactionsOk() {
        MerchantTransactionsListDTO merchantTransactionsListDTO = MerchantTransactionsListDTO.builder()
                .pageNo(0)
                .pageSize(10)
                .totalElements(1)
                .totalPages(1)
                .build();

        when(merchantTransactionService.getMerchantTransactions(
                eq("test"),
                eq("INITIATIVE_ID"),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                eq(paging)
        )).thenReturn(Mono.just(merchantTransactionsListDTO));

        Mono<MerchantTransactionsListDTO> resultMono = merchantTransactionController.getMerchantTransactions(
                "test",
                "INITIATIVE_ID",
                null,
                null,
                null,
                null,
                null,
                null,
                paging
        );

        MerchantTransactionsListDTO result = resultMono.block();

        assertSame(merchantTransactionsListDTO, result);
        assertEquals(0, result.getPageNo());
        assertEquals(10, result.getPageSize());
        assertEquals(1, result.getTotalElements());
        assertEquals(1, result.getTotalPages());

        verify(merchantTransactionService, times(1)).getMerchantTransactions(
                eq("test"),
                eq("INITIATIVE_ID"),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                isNull(),
                eq(paging)
        );
        verifyNoMoreInteractions(merchantTransactionService);
    }

    @Test
    void getProcessedTransactionStatusesOk() {
        String merchantId = "merchantId";
        String organizationRole = "ORG_ROLE";
        String initiativeId = "INITIATIVE_ID";

        List<String> statuses = List.of("AUTHORIZED", "REWARDED");

        when(merchantTransactionService.getProcessedTransactionStatuses(
                merchantId, organizationRole, initiativeId
        )).thenReturn(Mono.just(statuses));

        Mono<List<String>> resultMono = merchantTransactionController.getProcessedTransactionStatuses(
                merchantId, organizationRole, initiativeId
        );

        List<String> result = resultMono.block();

        assertEquals(statuses, result);

        verify(merchantTransactionService, times(1))
                .getProcessedTransactionStatuses(merchantId, organizationRole, initiativeId);
        verifyNoMoreInteractions(merchantTransactionService);
    }
}
