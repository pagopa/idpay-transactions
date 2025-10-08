package it.gov.pagopa.idpay.transactions.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.ClientException;
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
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
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

    PointOfSaleTransactionDTO dto = PointOfSaleTransactionDTOFaker.mockInstance(trx, INITIATIVE_ID, FISCAL_CODE);

    when(pointOfSaleTransactionService.getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), isNull(), isNull(), isNull(), any(Pageable.class)))
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
          assertEquals("TRX1", res.getContent().get(0).getTrxId());
          assertEquals(FISCAL_CODE, res.getContent().get(0).getFiscalCode());
          assertEquals(1, res.getTotalElements());
          assertEquals(1, res.getTotalPages());
          assertEquals(10, res.getPageSize());
        });
  }

    @Test
    void downloadInvoiceShouldReturnUrl() {
        doReturn(Mono.just(DownloadInvoiceResponseDTO.builder().invoiceUrl("testUrl").build()))
                .when(pointOfSaleTransactionService).downloadTransactionInvoice(
                        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(TRX_ID));
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/download")
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-initiative-id", INITIATIVE_ID)
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
          .exchange()
                .expectStatus().isOk()
                .expectBody(DownloadInvoiceResponseDTO.class)
                .value(res -> {
                   assertNotNull(res);
                   assertNotNull(res.getInvoiceUrl());
                   assertEquals("testUrl", res.getInvoiceUrl());
                   verify(pointOfSaleTransactionService).downloadTransactionInvoice(
                            eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(TRX_ID));
                });

    }

    @Test
    void downloadInvoiceShouldErrorOnServiceKO() {
        doReturn(Mono.error(new ClientExceptionNoBody(HttpStatus.BAD_REQUEST,
                ExceptionConstants.ExceptionMessage.TRANSACTION_MISSING_INVOICE)))
                .when(pointOfSaleTransactionService).downloadTransactionInvoice(
                        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(TRX_ID));
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/download")
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-initiative-id", INITIATIVE_ID)
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
                .exchange()
                .expectStatus().isBadRequest();

    }

    @Test
    void downloadInvoiceShouldReturnKOOnMissingMerchHeader() {
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/download")
                        .build(TRX_ID))
                .header("x-initiative-id", INITIATIVE_ID)
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void downloadInvoiceShouldReturnKOOnMissingInitHeader() {
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/download")
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-point-of-sale-id", POINT_OF_SALE_ID)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void downloadInvoiceShouldReturnKOOnMissingPoSHeader() {
        webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/idpay/transactions/{transactionId}/download")
                        .build(TRX_ID))
                .header("x-merchant-id", MERCHANT_ID)
                .header("x-initiative-id", INITIATIVE_ID)
                .exchange()
                .expectStatus().isBadRequest();
    }

}
