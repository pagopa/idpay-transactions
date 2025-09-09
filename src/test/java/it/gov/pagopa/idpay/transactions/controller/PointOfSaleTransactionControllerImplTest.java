package it.gov.pagopa.idpay.transactions.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionsListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.PointOfSaleTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.PointOfSaleTransactionService;
import it.gov.pagopa.idpay.transactions.test.fakers.PointOfSaleTransactionDTOFaker;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Pageable;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

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


  @Test
  void getPointOfSaleTransactions_defaultSortWithoutFiscalCode() {

    RewardTransaction trx1 = RewardTransactionFaker.mockInstance(1);
    trx1.setId("TRX1");
    trx1.setElaborationDateTime(trx1.getTrxDate().plusHours(2));
    trx1.setStatus("REWARDED");
    trx1.setEffectiveAmountCents(1000L);
    trx1.setRewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(500L).build()));

    RewardTransaction trx2 = RewardTransactionFaker.mockInstance(2);
    trx2.setId("TRX2");
    trx2.setElaborationDateTime(trx2.getTrxDate().plusHours(2));
    trx2.setStatus("REWARDED");
    trx2.setEffectiveAmountCents(1200L);
    trx2.setRewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(600L).build()));

    PointOfSaleTransactionDTO posTrx1 = PointOfSaleTransactionDTOFaker.mockInstance(trx1, INITIATIVE_ID, null);
    PointOfSaleTransactionDTO posTrx2 = PointOfSaleTransactionDTOFaker.mockInstance(trx2, INITIATIVE_ID, null);

    when(pointOfSaleTransactionService.getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(null), eq("REWARDED"), any(Pageable.class)))
        .thenReturn(Mono.just(Tuples.of(List.of(trx1, trx2), 2L)));

    when(mapper.toDTO(trx1, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx1));
    when(mapper.toDTO(trx2, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx2));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
            .queryParam("status", "REWARDED")
            .build(INITIATIVE_ID, POINT_OF_SALE_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(PointOfSaleTransactionsListDTO.class)
        .consumeWith(resp -> {
          PointOfSaleTransactionsListDTO body = resp.getResponseBody();
          assert body != null;
          assert body.getContent().size() == 2;
          assert body.getContent().get(0).getElaborationDateTime().equals(posTrx1.getElaborationDateTime());
          assert body.getContent().get(1).getElaborationDateTime().equals(posTrx2.getElaborationDateTime());
        });

    verify(pointOfSaleTransactionService).getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(null), eq("REWARDED"), any(Pageable.class));
    verify(mapper).toDTO(trx1, INITIATIVE_ID, null);
    verify(mapper).toDTO(trx2, INITIATIVE_ID, null);
  }

  @Test
  void getPointOfSaleTransactions_sortedByFiscalCodeAsc() {
    RewardTransaction trx1 = RewardTransactionFaker.mockInstance(1);
    trx1.setId("TRX1");
    trx1.setElaborationDateTime(trx1.getTrxDate().plusHours(2));
    trx1.setStatus("REWARDED");
    trx1.setFiscalCode("RNEVMP20A14A000M");
    trx1.setEffectiveAmountCents(1000L);
    trx1.setRewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(500L).build()));

    RewardTransaction trx2 = RewardTransactionFaker.mockInstance(2);
    trx2.setId("TRX2");
    trx2.setElaborationDateTime(trx2.getTrxDate().plusHours(2));
    trx2.setStatus("REWARDED");
    trx2.setFiscalCode("BVTVNL53E16A000T");
    trx2.setEffectiveAmountCents(1200L);
    trx2.setRewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(600L).build()));

    PointOfSaleTransactionDTO posTrx1 = PointOfSaleTransactionDTOFaker.mockInstance(trx1, INITIATIVE_ID, null);
    PointOfSaleTransactionDTO posTrx2 = PointOfSaleTransactionDTOFaker.mockInstance(trx2, INITIATIVE_ID, null);

    when(pointOfSaleTransactionService.getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(null), eq("REWARDED"), any(Pageable.class)))
        .thenReturn(Mono.just(Tuples.of(List.of(trx1, trx2), 2L)));

    when(mapper.toDTO(trx1, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx1));
    when(mapper.toDTO(trx2, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx2));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
            .queryParam("status", "REWARDED")
            .queryParam("sort", "fiscalCode,asc")
            .build(INITIATIVE_ID, POINT_OF_SALE_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(PointOfSaleTransactionsListDTO.class)
        .consumeWith(resp -> {
          PointOfSaleTransactionsListDTO body = resp.getResponseBody();
          assert body != null;
          assert body.getContent().size() == 2;
          assert body.getContent().get(0).getFiscalCode().equals("BVTVNL53E16A000T");
          assert body.getContent().get(1).getFiscalCode().equals("RNEVMP20A14A000M");
        });

    verify(pointOfSaleTransactionService).getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(null), eq("REWARDED"), any(Pageable.class));
    verify(mapper).toDTO(trx1, INITIATIVE_ID, null);
    verify(mapper).toDTO(trx2, INITIATIVE_ID, null);
  }

  @Test
  void getPointOfSaleTransactions_sortedByFiscalCodeDesc() {
    RewardTransaction trx1 = RewardTransactionFaker.mockInstance(1);
    trx1.setId("TRX1");
    trx1.setFiscalCode("BVTVNL53E16A000T");
    trx1.setElaborationDateTime(trx1.getTrxDate().plusHours(2));
    trx1.setStatus("REWARDED");

    RewardTransaction trx2 = RewardTransactionFaker.mockInstance(2);
    trx2.setId("TRX2");
    trx2.setFiscalCode("RNEVMP20A14A000M");
    trx2.setElaborationDateTime(trx2.getTrxDate().plusHours(2));
    trx2.setStatus("REWARDED");

    PointOfSaleTransactionDTO posTrx1 = PointOfSaleTransactionDTOFaker.mockInstance(trx1, INITIATIVE_ID, null);
    PointOfSaleTransactionDTO posTrx2 = PointOfSaleTransactionDTOFaker.mockInstance(trx2, INITIATIVE_ID, null);

    when(pointOfSaleTransactionService.getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(null), eq("REWARDED"), any(Pageable.class)))
        .thenReturn(Mono.just(Tuples.of(List.of(trx1, trx2), 2L)));

    when(mapper.toDTO(trx1, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx1));
    when(mapper.toDTO(trx2, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx2));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
            .queryParam("status", "REWARDED")
            .queryParam("sort", "fiscalCode,desc")
            .build(INITIATIVE_ID, POINT_OF_SALE_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(PointOfSaleTransactionsListDTO.class)
        .consumeWith(resp -> {
          PointOfSaleTransactionsListDTO body = resp.getResponseBody();
          assert body != null;
          assert body.getContent().size() == 2;
          assert body.getContent().get(0).getFiscalCode().equals("RNEVMP20A14A000M");
          assert body.getContent().get(1).getFiscalCode().equals("BVTVNL53E16A000T");
        });
  }

  @Test
  void getPointOfSaleTransactions_pagingSubList() {
    RewardTransaction trx1 = RewardTransactionFaker.mockInstance(1);
    trx1.setId("TRX1");
    trx1.setFiscalCode("BVTVNL53E16A000T");
    trx1.setElaborationDateTime(trx1.getTrxDate().plusHours(2));
    trx1.setStatus("REWARDED");

    RewardTransaction trx2 = RewardTransactionFaker.mockInstance(2);
    trx2.setId("TRX2");
    trx2.setFiscalCode("RNEVMP20A14A000M");
    trx2.setElaborationDateTime(trx2.getTrxDate().plusHours(2));
    trx2.setStatus("REWARDED");

    PointOfSaleTransactionDTO posTrx1 = PointOfSaleTransactionDTOFaker.mockInstance(trx1, INITIATIVE_ID, null);
    PointOfSaleTransactionDTO posTrx2 = PointOfSaleTransactionDTOFaker.mockInstance(trx2, INITIATIVE_ID, null);

    when(pointOfSaleTransactionService.getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(null), eq("REWARDED"), any(Pageable.class)))
        .thenReturn(Mono.just(Tuples.of(List.of(trx1, trx2), 2L)));

    when(mapper.toDTO(trx1, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx1));
    when(mapper.toDTO(trx2, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx2));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
            .queryParam("status", "REWARDED")
            .queryParam("sort", "fiscalCode,asc")
            .queryParam("page", "0")
            .queryParam("size", "1")
            .build(INITIATIVE_ID, POINT_OF_SALE_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(PointOfSaleTransactionsListDTO.class)
        .consumeWith(resp -> {
          PointOfSaleTransactionsListDTO body = resp.getResponseBody();
          assert body != null;
          assert body.getContent().size() == 1;
        });
  }

  @Test
  void getPointOfSaleTransactions_pagingEmptyList() {
    RewardTransaction trx1 = RewardTransactionFaker.mockInstance(1);
    trx1.setId("TRX1");
    trx1.setFiscalCode("BVTVNL53E16A000T");
    trx1.setElaborationDateTime(trx1.getTrxDate().plusHours(2));
    trx1.setStatus("REWARDED");

    RewardTransaction trx2 = RewardTransactionFaker.mockInstance(2);
    trx2.setId("TRX2");
    trx2.setFiscalCode("RNEVMP20A14A000M");
    trx2.setElaborationDateTime(trx2.getTrxDate().plusHours(2));
    trx2.setStatus("REWARDED");

    PointOfSaleTransactionDTO posTrx1 = PointOfSaleTransactionDTOFaker.mockInstance(trx1, INITIATIVE_ID, null);
    PointOfSaleTransactionDTO posTrx2 = PointOfSaleTransactionDTOFaker.mockInstance(trx2, INITIATIVE_ID, null);

    when(pointOfSaleTransactionService.getPointOfSaleTransactions(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(null), eq("REWARDED"), any(Pageable.class)))
        .thenReturn(Mono.just(Tuples.of(List.of(trx1, trx2), 2L)));

    when(mapper.toDTO(trx1, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx1));
    when(mapper.toDTO(trx2, INITIATIVE_ID, null)).thenReturn(Mono.just(posTrx2));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/initiatives/{initiativeId}/point-of-sales/{pointOfSaleId}/transactions/processed")
            .queryParam("status", "REWARDED")
            .queryParam("sort", "fiscalCode,asc")
            .queryParam("page", "10")
            .queryParam("size", "5")
            .build(INITIATIVE_ID, POINT_OF_SALE_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(PointOfSaleTransactionsListDTO.class)
        .consumeWith(resp -> {
          PointOfSaleTransactionsListDTO body = resp.getResponseBody();
          assert body != null;
          assert body.getContent().isEmpty();
        });
  }
}
