package it.gov.pagopa.idpay.transactions.controller;

import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.InvoiceLifecycleEligibilityResponse;
import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleEligibilityDecision;
import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleOperation;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.PaymentBatchEligibility;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.InvoiceLifecycleEligibilityService;
import it.gov.pagopa.idpay.transactions.service.RewardTransactionService;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.cache.CacheManager;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockUser;

@WebFluxTest(controllers = {TransactionsController.class})
class TransactionsControllerImplTest {
    @MockitoBean
    RewardTransactionService rewardTransactionService;
    @MockitoBean
    InvoiceLifecycleEligibilityService invoiceLifecycleEligibilityService;

    @Autowired
    protected WebTestClient webClient;
    @MockitoBean
    CacheManager cacheManager;


    @Test
    void findAllOk() {
        LocalDateTime now = LocalDateTime.of(2022, Month.SEPTEMBER, 20, 13, 15,45);
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId("USERID")
                .trxDate(now)
                .amountCents(3000L).build();

        //idTrxIssuer present in request
        when(rewardTransactionService.findByIdTrxIssuer(eq(rt.getIdTrxIssuer()),eq(rt.getUserId()), any(), any(), eq(rt.getAmountCents()), any()))
                .thenReturn(Flux.just(rt));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", rt.getIdTrxIssuer())
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);

        //userId e range of date present in the request
        when(rewardTransactionService.findByRange(eq(rt.getUserId()), any(), any(), eq(rt.getAmountCents()), any()))
                .thenReturn(Flux.just(rt));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);

        verify(rewardTransactionService, times(1)).findByIdTrxIssuer(any(), any(), any(), any(), any(), any());
    }

    @Test
    void findEligibilityReturnsAllEligibilityFields() {
        String merchantId = "MERCHANT_ID";
        String transactionId = "TRANSACTION_ID";
        PaymentBatchEligibility eligibility = new PaymentBatchEligibility(
                transactionId,
                "INITIATIVE_ID",
                merchantId,
                "REWARD_BATCH_ID",
                "INVOICED",
                RewardBatchStatus.EVALUATING,
                RewardBatchTrxStatus.SUSPENDED);

        when(rewardTransactionService.findEligibility(merchantId, transactionId))
                .thenReturn(Mono.just(eligibility));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/reward-batch/eligibility")
                        .queryParam("merchantId", merchantId)
                        .build(transactionId))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.transactionId").isEqualTo(transactionId)
                .jsonPath("$.initiativeId").isEqualTo("INITIATIVE_ID")
                .jsonPath("$.merchantId").isEqualTo(merchantId)
                .jsonPath("$.rewardBatchId").isEqualTo("REWARD_BATCH_ID")
                .jsonPath("$.transactionStatus").isEqualTo("INVOICED")
                .jsonPath("$.batchStatus").isEqualTo("EVALUATING")
                .jsonPath("$.batchTransactionStatus").isEqualTo("SUSPENDED");

        verify(rewardTransactionService).findEligibility(merchantId, transactionId);
    }

    @Test
    void findEligibilityReturnsNoContentWhenNoMembershipExists() {
        String merchantId = "MERCHANT_ID";
        String transactionId = "TRANSACTION_ID";
        when(rewardTransactionService.findEligibility(merchantId, transactionId))
                .thenReturn(Mono.empty());

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/reward-batch/eligibility")
                        .queryParam("merchantId", merchantId)
                        .build(transactionId))
                .exchange()
                .expectStatus().isNoContent()
                .expectBody().isEmpty();

        verify(rewardTransactionService).findEligibility(merchantId, transactionId);
    }

    @Test
    void findEligibilityPropagatesServiceErrors() {
        String merchantId = "MERCHANT_ID";
        String transactionId = "TRANSACTION_ID";
        when(rewardTransactionService.findEligibility(merchantId, transactionId))
                .thenReturn(Mono.error(new IllegalStateException("database unavailable")));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/reward-batch/eligibility")
                        .queryParam("merchantId", merchantId)
                        .build(transactionId))
                .exchange()
                .expectStatus().is5xxServerError();

        verify(rewardTransactionService).findEligibility(merchantId, transactionId);
    }

    @Test
    void findEligibilityRejectsMissingMerchantIdWithoutCallingService() {
        String transactionId = "TRANSACTION_ID";

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri("/idpay/transactions/{transactionId}/reward-batch/eligibility", transactionId)
                .exchange()
                .expectStatus().isBadRequest();

        verify(rewardTransactionService, never())
                .findEligibility(anyString(), anyString());
    }

    @ParameterizedTest
    @EnumSource(InvoiceLifecycleEligibilityDecision.class)
    void evaluateInvoiceLifecycleEligibilityReturnsDecisionOnly(
            InvoiceLifecycleEligibilityDecision decision
    ) {
        String merchantId = "MERCHANT_ID";
        String transactionId = "TRANSACTION_ID";
        String authorization = "Bearer token";
        when(invoiceLifecycleEligibilityService.evaluate(
                merchantId,
                transactionId,
                InvoiceLifecycleOperation.INVOICE_REPLACEMENT,
                authorization
        )).thenReturn(Mono.just(decision));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).post()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/invoice-lifecycle/eligibility")
                        .queryParam("merchantId", merchantId)
                        .build(transactionId))
                .header("Authorization", authorization)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue("""
                                {"operation":"INVOICE_REPLACEMENT"}
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectBody(InvoiceLifecycleEligibilityResponse.class)
                .isEqualTo(new InvoiceLifecycleEligibilityResponse(
                        decision
                ));

        verify(invoiceLifecycleEligibilityService).evaluate(
                merchantId,
                transactionId,
                InvoiceLifecycleOperation.INVOICE_REPLACEMENT,
                authorization
        );
    }

    @Test
    void evaluateInvoiceLifecycleEligibilityReturnsAllowedWithoutMembership() {
        String merchantId = "MERCHANT_ID";
        String transactionId = "TRANSACTION_ID";
        String authorization = "Bearer token";
        when(invoiceLifecycleEligibilityService.evaluate(
                merchantId,
                transactionId,
                InvoiceLifecycleOperation.INVOICED_REVERSAL,
                authorization
        )).thenReturn(Mono.just(InvoiceLifecycleEligibilityDecision.ALLOWED));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).post()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/invoice-lifecycle/eligibility")
                        .queryParam("merchantId", merchantId)
                        .build(transactionId))
                .header("Authorization", authorization)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue("""
                                {"operation":"INVOICED_REVERSAL"}
                        """)
                .exchange()
                        .expectStatus().isOk()
                        .expectBody(InvoiceLifecycleEligibilityResponse.class)
                        .isEqualTo(new InvoiceLifecycleEligibilityResponse(
                                InvoiceLifecycleEligibilityDecision.ALLOWED
                        ));

        verify(invoiceLifecycleEligibilityService).evaluate(
                merchantId,
                transactionId,
                InvoiceLifecycleOperation.INVOICED_REVERSAL,
                authorization
        );
    }

    @Test
    void evaluateInvoiceLifecycleEligibilityRejectsUnsupportedOperation() {
        webClient.mutateWith(mockUser()).mutateWith(csrf()).post()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/invoice-lifecycle/eligibility")
                        .queryParam("merchantId", "MERCHANT_ID")
                        .build("TRANSACTION_ID"))
                .header("Authorization", "Bearer token")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"operation":"UNSUPPORTED"}
                        """)
                .exchange()
                .expectStatus().isBadRequest();

        verify(invoiceLifecycleEligibilityService, never())
                .evaluate(anyString(), anyString(), any(), anyString());
    }

    @Test
    void evaluateInvoiceLifecycleEligibilityRejectsMissingOperation() {
        webClient.mutateWith(mockUser()).mutateWith(csrf()).post()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/invoice-lifecycle/eligibility")
                        .queryParam("merchantId", "MERCHANT_ID")
                        .build("TRANSACTION_ID"))
                .header("Authorization", "Bearer token")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isBadRequest();

        verify(invoiceLifecycleEligibilityService, never())
                .evaluate(anyString(), anyString(), any(), anyString());
    }

    @Test
    void findAllBadRequest(){
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .userId("USERID")
                .trxDate(now)
                .amountCents(3000L).build();

        ErrorDTO expectedErrorDTO = new ErrorDTO(ExceptionConstants.ExceptionCode.TRANSACTIONS_MISSING_MANDATORY_FILTERS, ExceptionConstants.ExceptionMessage.TRANSACTIONS_MISSING_MANDATORY_FILTERS);

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents()).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("userId", rt.getUserId())
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("amountCents", rt.getAmountCents())
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("trxDateStart", startDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("trxDateEnd", endDate).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("amountCents", rt.getAmountCents()).build())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(ErrorDTO.class).isEqualTo(expectedErrorDTO);

        verify(rewardTransactionService, never()).findByRange(any(), any(), any(), any(), any());
        verify(rewardTransactionService, never()).findByIdTrxIssuer(any(), any(), any(), any(), any(), any());
    }

    @Test
    void pageable(){
        LocalDateTime now = LocalDateTime.of(2022, Month.SEPTEMBER, 20, 13, 15,45);
        LocalDateTime startDate = now.minusMonths(5L);
        LocalDateTime endDate = now.plusMonths(8L);
        String userId = "USERID";
        String idTrxIssuer = "IDTRXISSUER";
        Long amountCents = 3000L;

        RewardTransaction rt = RewardTransaction.builder()
                .id("ID1")
                .idTrxIssuer(idTrxIssuer)
                .userId(userId)
                .trxDate(now)
                .amountCents(amountCents).build();

        //idTrxIssuer present in request
        when(rewardTransactionService.findByIdTrxIssuer(any(),any(), any(), any(), eq(amountCents), any()))
                .thenReturn(Flux.just(rt));

        Pageable expectedPageable = PageRequest.of(2, 3, Sort.unsorted());

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", idTrxIssuer)
                        .queryParam("userId", userId)
                        .queryParam("amountCents", amountCents)
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .queryParam("page", expectedPageable.getPageNumber())
                        .queryParam("size", expectedPageable.getPageSize())
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);
        verify(rewardTransactionService, times(1)).findByIdTrxIssuer(eq(rt.getIdTrxIssuer()), any(), any(),any(),any(),eq(expectedPageable));

        Pageable expectedPageable2 = PageRequest.of(0, 3, Sort.Direction.DESC, "_id");
        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions")
                        .queryParam("idTrxIssuer", "idTrxIssuer2")
                        .queryParam("userId", userId)
                        .queryParam("amountCents", amountCents)
                        .queryParam("trxDateStart", startDate)
                        .queryParam("trxDateEnd", endDate)
                        .queryParam("page", expectedPageable2.getPageNumber())
                        .queryParam("size", expectedPageable2.getPageSize())
                        .queryParam("sort", "_id,desc")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);
        verify(rewardTransactionService, times(1)).findByIdTrxIssuer(eq("idTrxIssuer2"), any(),any(),any(),any(),eq(expectedPageable2));

    }

    @Test
    void findByInitiativeIdAndUserId_Ok() {
        LocalDateTime now = LocalDateTime.of(2022, Month.SEPTEMBER, 20, 13, 15,45);

        RewardTransaction rt = RewardTransaction.builder()
                .idTrxIssuer("IDTRXISSUER")
                .initiatives(List.of("ID"))
                .userId("USERID")
                .trxDate(now)
                .amountCents(3000L).build();


        when(rewardTransactionService.findByInitiativeIdAndUserId("ID","USERID"))
                .thenReturn(Flux.just(rt));

        webClient.mutateWith(mockUser()).mutateWith(csrf()).get()
                .uri(uriBuilder -> uriBuilder.path("/idpay/transactions/ID/USERID")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(RewardTransaction.class).contains(rt);

    }

    @Test
    void cleanupInvoicedTransactions_defaultChunkSize() {
        when(rewardTransactionService.assignInvoicedTransactionsToBatches(anyInt(),
                        anyInt(), anyBoolean(), isNull()))
                .thenReturn(Mono.empty());

        webClient.mutateWith(mockUser()).mutateWith(csrf()).post()
                .uri("/idpay/transactions/cleanup")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(String.class);

        verify(rewardTransactionService, times(1))
                .assignInvoicedTransactionsToBatches(
                        eq(200),
                        eq(1),
                        eq(false),
                        isNull());
    }

    @Test
    void cleanupInvoicedTransactions_customChunkSize() {
        when(rewardTransactionService.assignInvoicedTransactionsToBatches(anyInt(),
                        anyInt(), anyBoolean(), isNull()))
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

        verify(rewardTransactionService, times(1))
                .assignInvoicedTransactionsToBatches(
                        eq(customChunkSize),
                        eq(customIteration), eq(false),
                        isNull()
                );
    }
}