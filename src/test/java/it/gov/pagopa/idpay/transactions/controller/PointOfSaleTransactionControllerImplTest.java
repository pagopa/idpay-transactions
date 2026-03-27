package it.gov.pagopa.idpay.transactions.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.service.invoice_lifecycle.InvoiceLifecyclePolicy;
import it.gov.pagopa.idpay.transactions.test.fakers.PointOfSaleTransactionDTOFaker;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Mono;

@WebFluxTest(controllers = PointOfSaleTransactionControllerImpl.class)
class PointOfSaleTransactionControllerImplTest {

    @Autowired
    protected WebTestClient webClient;

    @MockitoBean
    PointOfSaleTransactionService pointOfSaleTransactionService;

    @MockitoBean
    PointOfSaleTransactionMapper mapper;

    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String POINT_OF_SALE_ID = "POINT_OF_SALE_ID";
    private static final String MERCHANT_ID = "MERCHANT_ID";
    private static final String FISCAL_CODE = "FISCALCODE1";
    private static final String TRX_ID = "TRX_ID_1";
    private static final String PRODUCT_GTIN = "GTIN123";
    private static final String STATUS = "REWARDED";
    private static final String TRX_CODE = "TRXCODE123";
    private static final String REWARD_BATCH_ID = "REWARD_BATCH_ID";

    @Test
    void getPointOfSaleTransactionsOk() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setId("TRX1");

        Page<RewardTransaction> page = new PageImpl<>(
                List.of(trx),
                PageRequest.of(0, 10), 1
        );

        PointOfSaleTransactionDTO dto =
                PointOfSaleTransactionDTOFaker.mockInstance(trx, INITIATIVE_ID, FISCAL_CODE);

        when(pointOfSaleTransactionService.getPointOfSaleTransactions(
                eq(MERCHANT_ID),
                eq(INITIATIVE_ID),
                eq(POINT_OF_SALE_ID),
                eq(PRODUCT_GTIN),
                any(TrxFiltersDTO.class),
                any(Pageable.class)))
                .thenReturn(Mono.just(page));

        when(mapper.toDTO(eq(trx), INITIATIVE_ID, FISCAL_CODE))
                .thenReturn(Mono.just(dto));

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
                        .queryParam("productGtin", PRODUCT_GTIN)
                        .queryParam("fiscalCode", FISCAL_CODE)
                        .queryParam("status", STATUS)
                        .queryParam("trxCode", TRX_CODE)
                        .build(INITIATIVE_ID, POINT_OF_SALE_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody(PointOfSaleTransactionsListDTO.class)
                .value(res -> {
                    assertNotNull(res);
                    assertEquals(1, res.getContent().size());
                    assertEquals("TRX1", res.getContent().getFirst().getTrxId());
                    assertEquals(FISCAL_CODE, res.getContent().getFirst().getFiscalCode());
                    assertEquals(1, res.getTotalElements());
                    assertEquals(1, res.getTotalPages());
                    assertEquals(10, res.getPageSize());
                    assertEquals(0, res.getPageNo());
                });

        verify(pointOfSaleTransactionService).getPointOfSaleTransactions(
                eq(MERCHANT_ID),
                eq(INITIATIVE_ID),
                eq(POINT_OF_SALE_ID),
                eq(PRODUCT_GTIN),
                argThat(filters ->
                        FISCAL_CODE.equals(filters.getFiscalCode())
                                && STATUS.equals(filters.getStatus())
                                && TRX_CODE.equals(filters.getTrxCode())),
                any(Pageable.class));

        verify(mapper).toDTO(eq(trx), INITIATIVE_ID, eq(FISCAL_CODE));
    }

    @Test
    void getPointOfSaleTransactionsOkWithoutPosHeader() {
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setId("TRX2");

        Page<RewardTransaction> page = new PageImpl<>(
                List.of(trx),
                PageRequest.of(0, 10), 1
        );

        PointOfSaleTransactionDTO dto =
                PointOfSaleTransactionDTOFaker.mockInstance(trx, INITIATIVE_ID, null);

        when(pointOfSaleTransactionService.getPointOfSaleTransactions(
                eq(MERCHANT_ID),
                eq(INITIATIVE_ID),
                eq(POINT_OF_SALE_ID),
                isNull(),
                any(TrxFiltersDTO.class),
                any(Pageable.class)))
                .thenReturn(Mono.just(page));

        when(mapper.toDTO(eq(trx), eq(INITIATIVE_ID), isNull()))
                .thenReturn(Mono.just(dto));

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
                        .build(INITIATIVE_ID, POINT_OF_SALE_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody(PointOfSaleTransactionsListDTO.class)
                .value(res -> {
                    assertNotNull(res);
                    assertEquals(1, res.getContent().size());
                    assertEquals("TRX2", res.getContent().getFirst().getTrxId());
                });
    }

    @Test
    void getPointOfSaleTransactionsForbidden() {
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
                        .build(INITIATIVE_ID, POINT_OF_SALE_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-point-of-sale-id", "ALTRO_POS")
                .exchange()
                .expectStatus().isForbidden();

        verifyNoInteractions(pointOfSaleTransactionService);
        verifyNoInteractions(mapper);
    }

    @Test
    void downloadInvoiceShouldReturnUrl() {
        doReturn(Mono.just(DownloadInvoiceResponseDTO.builder().invoiceUrl("testUrl").build()))
                .when(pointOfSaleTransactionService)
                .downloadTransactionInvoice(INITIATIVE_ID, MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(POINT_OF_SALE_ID, TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody(DownloadInvoiceResponseDTO.class)
                .value(res -> {
                    assertNotNull(res);
                    assertNotNull(res.getInvoiceUrl());
                    assertEquals("testUrl", res.getInvoiceUrl());
                });

        verify(pointOfSaleTransactionService)
                .downloadTransactionInvoice(INITIATIVE_ID, MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    }

    @Test
    void downloadInvoiceShouldErrorOnServiceKO() {
        doReturn(Mono.error(new ClientExceptionNoBody(
                HttpStatus.BAD_REQUEST,
                ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE)))
                .when(pointOfSaleTransactionService)
                .downloadTransactionInvoice(INITIATIVE_ID, MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(POINT_OF_SALE_ID, TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void downloadInvoiceShouldReturnKOOnMissingMerchHeader() {
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(POINT_OF_SALE_ID, TRX_ID))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void downloadInvoiceShouldReturnForbiddenOnPosMismatch() {
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(POINT_OF_SALE_ID, TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-point-of-sale-id", "ALTRO_POS")
                .exchange()
                .expectStatus().isForbidden();

        verify(pointOfSaleTransactionService, never())
                .downloadTransactionInvoice(anyString(), anyString(), anyString(), anyString());
    }

    @Test
    void downloadInvoiceShouldReturnOkWithoutPosHeader() {
        doReturn(Mono.just(DownloadInvoiceResponseDTO.builder().invoiceUrl("testUrl").build()))
                .when(pointOfSaleTransactionService)
                .downloadTransactionInvoice(INITIATIVE_ID, MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(POINT_OF_SALE_ID, TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBody(DownloadInvoiceResponseDTO.class)
                .value(res -> {
                    assertNotNull(res);
                    assertEquals("testUrl", res.getInvoiceUrl());
                });
    }

    @Test
    void updateInvoiceFileOk() {
        String authorization = buildBearerTokenWithScope("transaction:invoicelifecycle:full");

        when(pointOfSaleTransactionService.updateInvoiceTransaction(
                eq(INITIATIVE_ID),
                eq(TRX_ID),
                eq(MERCHANT_ID),
                any(FilePart.class),
                eq("DOC123"),
                any(InvoiceLifecyclePolicy.class)))
                .thenReturn(Mono.empty());

        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("file", "dummycontent".getBytes())
                .filename("invoice.pdf")
                .contentType(MediaType.APPLICATION_OCTET_STREAM);
        builder.part("docNumber", "DOC123");

        webClient.put()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{id}/invoice/update")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
                .header("Authorization", authorization)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(builder.build()))
                .exchange()
                .expectStatus().isNoContent();

        verify(pointOfSaleTransactionService).updateInvoiceTransaction(
                eq(INITIATIVE_ID),
                eq(TRX_ID),
                eq(MERCHANT_ID),
                any(FilePart.class),
                eq("DOC123"),
                any(InvoiceLifecyclePolicy.class));
    }

    @Test
    void updateInvoiceFileBadRequestWhenMissingMerchantId() {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("file", "dummy".getBytes())
                .filename("f.pdf")
                .contentType(MediaType.APPLICATION_OCTET_STREAM);

        webClient.put()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/invoice/update")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(TRX_ID))
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
                .header("Authorization", buildBearerTokenWithScope("transaction:invoicelifecycle:full"))
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(builder.build()))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void updateInvoiceFileShouldReturnInternalServerErrorWhenAuthorizationIsInvalid() {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("file", "dummycontent".getBytes())
                .filename("invoice.pdf")
                .contentType(MediaType.APPLICATION_OCTET_STREAM);
        builder.part("docNumber", "DOC123");

        webClient.put()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{id}/invoice/update")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("Authorization", "Bearer invalid.token.value")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(builder.build()))
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

        verify(pointOfSaleTransactionService, never()).updateInvoiceTransaction(
                anyString(), anyString(), anyString(), any(), any(), any());
    }

    @Test
    void getFranchisePointOfSaleOk() {
        List<FranchisePointOfSaleDTO> response = List.of(new FranchisePointOfSaleDTO());

        when(pointOfSaleTransactionService.getDistinctFranchiseAndPosByRewardBatchId(
                INITIATIVE_ID, MERCHANT_ID, REWARD_BATCH_ID))
                .thenReturn(Mono.just(response));

        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/point-of-sales/{rewardBatchId}")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(REWARD_BATCH_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(FranchisePointOfSaleDTO.class)
                .value(res -> assertEquals(1, res.size()));

        verify(pointOfSaleTransactionService)
                .getDistinctFranchiseAndPosByRewardBatchId(INITIATIVE_ID, MERCHANT_ID, REWARD_BATCH_ID);
    }

    @Test
    void reversalTransactionOk() {
        when(pointOfSaleTransactionService.reversalTransaction(
                eq(INITIATIVE_ID),
                eq(TRX_ID),
                eq(MERCHANT_ID),
                any(FilePart.class),
                eq("DOC456"),
                any(InvoiceLifecyclePolicy.class)))
                .thenReturn(Mono.empty());

        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("file", "dummycontent".getBytes())
                .filename("reversal.pdf")
                .contentType(MediaType.APPLICATION_OCTET_STREAM);
        builder.part("docNumber", "DOC456");

        webClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{id}/reversal-invoiced")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
                .header("Authorization", buildBearerTokenWithScope("transaction:invoicelifecycle:basic"))
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(builder.build()))
                .exchange()
                .expectStatus().isNoContent()
                .expectBody().isEmpty();

        verify(pointOfSaleTransactionService).reversalTransaction(
                eq(INITIATIVE_ID),
                eq(TRX_ID),
                eq(MERCHANT_ID),
                any(FilePart.class),
                eq("DOC456"),
                any(InvoiceLifecyclePolicy.class));
    }

    @Test
    void reversalTransactionShouldReturnInternalServerErrorWhenAuthorizationIsInvalid() {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("file", "dummycontent".getBytes())
                .filename("reversal.pdf")
                .contentType(MediaType.APPLICATION_OCTET_STREAM);
        builder.part("docNumber", "DOC456");

        webClient.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{id}/reversal-invoiced")
                        .queryParam("initiativeId", INITIATIVE_ID)
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("Authorization", "Bearer invalid.token.value")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(builder.build()))
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

        verify(pointOfSaleTransactionService, never()).reversalTransaction(
                anyString(), anyString(), anyString(), any(), any(), any());
    }

    private static String buildBearerTokenWithScope(String scope) {
        String header = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
        String payload = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(("{\"scope\":\"" + scope + "\"}").getBytes(StandardCharsets.UTF_8));
        return "Bearer " + header + "." + payload + ".sig";
    }
}