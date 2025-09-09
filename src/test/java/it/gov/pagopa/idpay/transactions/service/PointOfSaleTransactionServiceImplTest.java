package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionServiceImplTest {

  @Mock
  private UserRestClient userRestClient;

  @Mock
  private RewardTransactionRepository rewardTransactionRepository;

  private PointOfSaleTransactionService pointOfSaleTransactionService;

  private static final String MERCHANT_ID = "MERCHANTID1";
  private static final String INITIATIVE_ID = "INITIATIVEID1";
  private static final String POINT_OF_SALE_ID = "POINTOFSALEID1";
  private static final String FISCAL_CODE = "FISCALCODE1";
  private static final String USER_ID = "USERID1";
  private static final String STATUS = "REWARDED";

  @BeforeEach
  void setUp(){
    pointOfSaleTransactionService = new PointOfSaleTransactionServiceImpl(userRestClient, rewardTransactionRepository);
  }

  @Test
  void getPointOfSaleTransactions_withFiscalCode_shouldResolveUserId() {
    RewardTransaction trx1 = RewardTransactionFaker.mockInstance(1);
    RewardTransaction trx2 = RewardTransactionFaker.mockInstance(2);

    when(userRestClient.retrieveFiscalCodeInfo(FISCAL_CODE))
        .thenReturn(Mono.just(new FiscalCodeInfoPDV(USER_ID)));
    when(rewardTransactionRepository.findByFilterTrx(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), eq(USER_ID), eq(STATUS), any())
    ).thenReturn(Flux.just(trx1, trx2));
    when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, STATUS))
        .thenReturn(Mono.just(2L));

    Tuple2<List<RewardTransaction>, Long> result =
        pointOfSaleTransactionService.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, FISCAL_CODE, STATUS, Pageable.unpaged()).block();

    assertNotNull(result);
    assertEquals(2, result.getT1().size());
    assertEquals(trx1.getIdTrxAcquirer(), result.getT1().get(0).getIdTrxAcquirer());
    assertEquals(trx2.getIdTrxAcquirer(), result.getT1().get(1).getIdTrxAcquirer());
    assertEquals(2L, result.getT2());

    verify(userRestClient).retrieveFiscalCodeInfo(FISCAL_CODE);
  }

  @Test
  void getPointOfSaleTransactions_noFiscalCodeFilter() {
    RewardTransaction trx1 = RewardTransactionFaker.mockInstance(1);
    RewardTransaction trx2 = RewardTransactionFaker.mockInstance(2);

    when(rewardTransactionRepository.findByFilterTrx(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), isNull(), eq(STATUS), any())
    ).thenReturn(Flux.just(trx1, trx2));
    when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS))
        .thenReturn(Mono.just(2L));

    Tuple2<List<RewardTransaction>, Long> result =
        pointOfSaleTransactionService.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS, Pageable.unpaged()).block();

    assertNotNull(result);
    assertEquals(2, result.getT1().size());
    assertEquals(trx1.getIdTrxAcquirer(), result.getT1().get(0).getIdTrxAcquirer());
    assertEquals(trx2.getIdTrxAcquirer(), result.getT1().get(1).getIdTrxAcquirer());
    assertEquals(2L, result.getT2());

    verifyNoInteractions(userRestClient);
  }

  @Test
  void getPointOfSaleTransactions_withSortOnFiscalCode_shouldPassNullPageable() {
    RewardTransaction trx = RewardTransactionFaker.mockInstance(3);

    Pageable pageable = PageRequest.of(0, 10, Sort.by("fiscalCode").ascending());

    when(rewardTransactionRepository.findByFilterTrx(
        eq(MERCHANT_ID), eq(INITIATIVE_ID), eq(POINT_OF_SALE_ID), isNull(), eq(STATUS), isNull())
    ).thenReturn(Flux.just(trx));
    when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS))
        .thenReturn(Mono.just(1L));

    Tuple2<List<RewardTransaction>, Long> result =
        pointOfSaleTransactionService.getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS, pageable).block();

    assertNotNull(result);
    assertEquals(1, result.getT1().size());
    assertEquals(trx.getIdTrxAcquirer(), result.getT1().get(0).getIdTrxAcquirer());

    verify(rewardTransactionRepository).findByFilterTrx(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS, null);
  }

  @Test
  void getTransactions_withNullPageable_shouldCallRepositoryWithNonNullRepoPageable() {
    RewardTransaction trx = RewardTransactionFaker.mockInstance(1);

    when(rewardTransactionRepository.findByFilterTrx(
        anyString(), anyString(), anyString(), any(), anyString(), any()))
        .thenReturn(Flux.just(trx));

    when(rewardTransactionRepository.getCount(anyString(), anyString(), anyString(), any(), anyString()))
        .thenReturn(Mono.just(1L));

    Tuple2<List<RewardTransaction>, Long> result =
        pointOfSaleTransactionService.getPointOfSaleTransactions(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS, null)
            .block();

    assertNotNull(result);
    assertEquals(1, result.getT2());
    assertEquals(trx.getId(), result.getT1().get(0).getId());

    verify(rewardTransactionRepository, times(1))
        .findByFilterTrx(anyString(), anyString(), anyString(), any(), anyString(), any());
  }
}
