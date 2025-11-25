package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.idpay.transactions.dto.MerchantTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.enums.OrganizationRole;
import it.gov.pagopa.idpay.transactions.service.MerchantTransactionService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Pageable;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@WebFluxTest(controllers = MerchantTransactionControllerImpl.class)
class MerchantTransactionControllerImplTest {

    private static final String MERCHANT_ID = "3a602b17-ac1c-3029-9e78-0a4bbb8693d4";
    private static final String INITIATIVE_ID = "68dd003ccce8c534d1da22bc";

    @TestConfiguration
    static class Config {

        @Bean
        public MerchantTransactionService merchantTransactionService() {
            return Mockito.mock(MerchantTransactionService.class);
        }
    }

    @Autowired
    WebTestClient webTestClient;

    @Autowired
    MerchantTransactionService merchantTransactionService;

    @Test
    void merchantRole_successAndForbiddenOnToCheck() {
        webTestClient.get()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed", INITIATIVE_ID)
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-organization-role", "MERCHANT")
                .exchange()
                .expectStatus().isOk();

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed")
                        .queryParam("rewardBatchTrxStatus", "TO_CHECK")
                        .build(INITIATIVE_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-organization-role", "MERCHANT")
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody()
                .jsonPath("$.code").isEqualTo("Error")
                .jsonPath("$.message").isEqualTo("Something gone wrong");
    }



    @Test
    void invitaliaRole_allowsToCheck() {
        MerchantTransactionsListDTO response = MerchantTransactionsListDTO.builder()
                .pageNo(0)
                .pageSize(10)
                .totalElements(0)
                .totalPages(0)
                .build();

        when(merchantTransactionService.getMerchantTransactions(
                eq(MERCHANT_ID),
                eq(OrganizationRole.INVITALIA),
                eq(INITIATIVE_ID),
                isNull(),
                isNull(),
                isNull(),
                eq("TO_CHECK"),
                any(Pageable.class)
        )).thenReturn(Mono.just(response));

        webTestClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed")
                        .queryParam("rewardBatchTrxStatus", "TO_CHECK")
                        .build(INITIATIVE_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-organization-role", "INVITALIA")
                .exchange()
                .expectStatus().isOk()
                .expectBody(MerchantTransactionsListDTO.class)
                .isEqualTo(response);

        verify(merchantTransactionService).getMerchantTransactions(
                eq(MERCHANT_ID),
                eq(OrganizationRole.INVITALIA),
                eq(INITIATIVE_ID),
                isNull(),
                isNull(),
                isNull(),
                eq("TO_CHECK"),
                any(Pageable.class)
        );
    }
    @Test
    void getProcessedTransactionStatuses_merchant_doesNotContainToCheck() {
        webTestClient.get()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed/statuses",
                        "68dd003ccce8c534d1da22bc")
                .header("x-merchant-id", "3a602b17-ac1c-3029-9e78-0a4bbb8693d4")
                .header("x-organization-role", "MERCHANT")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(String.class)
                .value(statuses -> {
                    assertThat(statuses)
                            .isNotEmpty()
                            .doesNotContain("TO_CHECK");
                });
    }

    @Test
    void getProcessedTransactionStatuses_invitalia_containsToCheck() {
        webTestClient.get()
                .uri("/idpay/merchant/portal/initiatives/{initiativeId}/transactions/processed/statuses",
                        INITIATIVE_ID)
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-organization-role", OrganizationRole.INVITALIA.name())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[?(@ == 'TO_CHECK')]").exists();
    }

}
