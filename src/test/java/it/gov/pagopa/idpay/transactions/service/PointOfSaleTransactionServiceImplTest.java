package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.FiscalCodeInfoPDV;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class PointOfSaleTransactionServiceImplTest {

  @Mock
  private UserRestClient userRestClient;

  @Mock
  private RewardTransactionRepository rewardTransactionRepository;

  private PointOfSaleTransactionService pointOfSaleTransactionService;

  private final Pageable pageable = PageRequest.of(0, 10);

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
  void getPointOfSaleTransactionsWithFiscalCode() {
    RewardTransaction trx = RewardTransactionFaker.mockInstance(1);

    when(userRestClient.retrieveFiscalCodeInfo(FISCAL_CODE)).thenReturn(Mono.just(new FiscalCodeInfoPDV(USER_ID)));

    when(rewardTransactionRepository.findByFilterTrx(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, STATUS, pageable)).thenReturn(Flux.just(trx));

    when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, STATUS))
        .thenReturn(Mono.just(1L));

    Mono<Page<RewardTransaction>> result = pointOfSaleTransactionService
        .getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, FISCAL_CODE, STATUS, pageable);

    StepVerifier.create(result)
        .assertNext(page -> {
          assertEquals(1, page.getTotalElements());
          assertEquals(1, page.getContent().size());
          assertEquals(trx.getIdTrxAcquirer(), page.getContent().get(0).getIdTrxAcquirer());
        })
        .verifyComplete();
  }

  @Test
  void getPointOfSaleTransactionsWithoutFiscalCode() {
    RewardTransaction trx = RewardTransactionFaker.mockInstance(2);

    when(rewardTransactionRepository.findByFilterTrx(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS, pageable))
        .thenReturn(Flux.just(trx));

    when(rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS))
        .thenReturn(Mono.just(1L));

    Mono<Page<RewardTransaction>> result = pointOfSaleTransactionService
        .getPointOfSaleTransactions(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, STATUS, pageable);

    StepVerifier.create(result)
        .assertNext(page -> {
          assertEquals(1, page.getTotalElements());
          assertEquals(1, page.getContent().size());
          assertEquals(trx.getIdTrxAcquirer(), page.getContent().get(0).getIdTrxAcquirer());
        })
        .verifyComplete();
  }
}
