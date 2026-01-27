package it.gov.pagopa.idpay.transactions.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.dto.DownloadInvoiceResponseDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.test.fakers.PointOfSaleTransactionDTOFaker;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import java.util.List;

import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants;
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


  @Test
  void getPointOfSaleTransactionsOk() {
    RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
    trx.setId("TRX1");

    Page<RewardTransaction> page = new PageImpl<>(
        List.of(trx),
        PageRequest.of(0, 10), 1
    );

    PointOfSaleTransactionDTO dto = PointOfSaleTransactionDTOFaker
        .mockInstance(trx, INITIATIVE_ID, FISCAL_CODE);

    when(pointOfSaleTransactionService.getPointOfSaleTransactions(
        eq(MERCHANT_ID),
        eq(INITIATIVE_ID),
        eq(POINT_OF_SALE_ID),
        isNull(),
        isNull(),
        isNull(),
        isNull(),
        any(Pageable.class)))
        .thenReturn(Mono.just(page));

    when(mapper.toDTO(eq(trx), eq(INITIATIVE_ID), isNull()))
        .thenReturn(Mono.just(dto));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
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
        });
  }

  @Test
  void downloadInvoiceShouldReturnUrl() {
    doReturn(Mono.just(DownloadInvoiceResponseDTO.builder().invoiceUrl("testUrl").build()))
        .when(pointOfSaleTransactionService).downloadTransactionInvoice(
            MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
            .build(POINT_OF_SALE_ID, TRX_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(DownloadInvoiceResponseDTO.class)
        .value(res -> {
          assertNotNull(res);
          assertNotNull(res.getInvoiceUrl());
          assertEquals("testUrl", res.getInvoiceUrl());
          verify(pointOfSaleTransactionService).downloadTransactionInvoice(
              MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
        });

  }

  @Test
  void downloadInvoiceShouldErrorOnServiceKO() {
    doReturn(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST,
        ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE)))
        .when(pointOfSaleTransactionService).downloadTransactionInvoice(
            MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);
    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
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
            .build(POINT_OF_SALE_ID, TRX_ID))
        .exchange()
        .expectStatus().isBadRequest();
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
  }

  @Test
  void downloadInvoiceShouldReturnForbiddenOnPosMismatch() {
    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
            .build(POINT_OF_SALE_ID, TRX_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .header("x-point-of-sale-id", "ALTRO_POS")
        .exchange()
        .expectStatus().isForbidden();
  }

  @Test
  void downloadInvoiceShouldReturnOkWithoutPosHeader() {
    doReturn(Mono.just(DownloadInvoiceResponseDTO.builder().invoiceUrl("testUrl").build()))
        .when(pointOfSaleTransactionService).downloadTransactionInvoice(
            MERCHANT_ID, POINT_OF_SALE_ID, TRX_ID);

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/{pointOfSaleId}/transactions/{transactionId}/download")
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

    when(pointOfSaleTransactionService.updateInvoiceTransaction(
        eq(TRX_ID),
        eq(MERCHANT_ID),
        eq(POINT_OF_SALE_ID),
        any(FilePart.class),
        eq("DOC123")
    )).thenReturn(Mono.empty());

    MultipartBodyBuilder builder = new MultipartBodyBuilder();
    builder.part("file", "dummycontent".getBytes())
        .filename("invoice.pdf")
        .contentType(MediaType.APPLICATION_OCTET_STREAM);
    builder.part("docNumber", "DOC123");

    webClient.put()
        .uri("/idpay/transactions/{id}/invoice/update", TRX_ID)
        .header("x-merchant-id", MERCHANT_ID)
        .header("x-point-of-sale-id", POINT_OF_SALE_ID)
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(builder.build()))
        .exchange()
        .expectStatus().isNoContent();

    verify(pointOfSaleTransactionService).updateInvoiceTransaction(
        eq(TRX_ID),
        eq(MERCHANT_ID),
        eq(POINT_OF_SALE_ID),
        any(FilePart.class),
        eq("DOC123")
    );
  }

  @Test
  void updateInvoiceFileBadRequestWhenMissingMerchantId() {
    MultipartBodyBuilder builder = new MultipartBodyBuilder();
    builder.part("file", "dummy".getBytes())
        .filename("f.pdf")
        .contentType(MediaType.APPLICATION_OCTET_STREAM);

    webClient.put()
        .uri("/idpay/transactions/{transactionId}/invoice/update", TRX_ID)
        .header("x-point-of-sale-id", POINT_OF_SALE_ID)
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(builder.build()))
        .exchange()
        .expectStatus().isBadRequest();
  }

  @Test
  void reversalTransactionOk() {

    when(pointOfSaleTransactionService.reversalTransaction(
        eq(TRX_ID),
        eq(MERCHANT_ID),
        eq(POINT_OF_SALE_ID),
        any(FilePart.class),
        eq("DOC456")
    )).thenReturn(Mono.empty());

    MultipartBodyBuilder builder = new MultipartBodyBuilder();
    builder.part("file", "dummycontent".getBytes())
        .filename("reversal.pdf")
        .contentType(MediaType.APPLICATION_OCTET_STREAM);
    builder.part("docNumber", "DOC456");

    webClient.post()
        .uri("/idpay/transactions/{id}/reversal-invoiced", TRX_ID)
        .header("x-merchant-id", MERCHANT_ID)
        .header("x-point-of-sale-id", POINT_OF_SALE_ID)
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(builder.build()))
        .exchange()
        .expectStatus().isNoContent();

    verify(pointOfSaleTransactionService).reversalTransaction(
        eq(TRX_ID),
        eq(MERCHANT_ID),
        eq(POINT_OF_SALE_ID),
        any(FilePart.class),
        eq("DOC456")
    );
  }

}
