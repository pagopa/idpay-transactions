package it.gov.pagopa.idpay.transactions.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.test.fakers.PointOfSaleTransactionDTOFaker;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@WebFluxTest(controllers = PointOfSaleTransactionControllerImpl.class)
class PointOfSaleTransactionControllerImplTest {

  @Autowired
  protected WebTestClient webClient;

  @MockBean
  PointOfSaleTransactionService pointOfSaleTransactionService;

  @MockBean
  PointOfSaleTransactionMapper mapper;

  private static final String INITIATIVE_ID = "INITIATIVE_ID";
  private static final String POINT_OF_SALE_ID = "POINT_OF_SALE_ID";
  private static final String MERCHANT_ID = "MERCHANT_ID";
  private static final String FISCAL_CODE = "FISCALCODE1";


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
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), isNull(), isNull(), any(Pageable.class)))
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
}
