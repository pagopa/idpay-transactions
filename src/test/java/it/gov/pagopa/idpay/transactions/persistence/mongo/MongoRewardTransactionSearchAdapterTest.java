package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardTransactionSearchAdapterTest {

    private static final String MERCHANT_ID = "merchant";
    private static final String INITIATIVE_ID = "initiative";
    private static final String POS_ID = "pos";
    private static final String USER_ID = "user";
    private static final String PRODUCT_GTIN = "gtin";

    @Mock
    private RewardTransactionRepository rewardTransactionRepository;

    @Test
    void merchantSearchAndCount_delegateFiscalCodeAndConsultableVisibility() {
        MongoRewardTransactionSearchAdapter adapter =
                new MongoRewardTransactionSearchAdapter(rewardTransactionRepository);
        TrxFiltersDTO filters = filters();
        PageRequest pageable = PageRequest.of(1, 20);
        RewardTransaction transaction = RewardTransaction.builder().id("transaction").build();

        when(rewardTransactionRepository.findByFilter(filters, USER_ID, true, pageable))
                .thenReturn(Flux.just(transaction));
        when(rewardTransactionRepository.getCount(filters, POS_ID, null, USER_ID, true))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(adapter.findMerchantTransactions(filters, USER_ID, true, pageable))
                .expectNext(transaction)
                .verifyComplete();
        StepVerifier.create(adapter.countMerchantTransactions(filters, USER_ID, true))
                .expectNext(1L)
                .verifyComplete();

        verify(rewardTransactionRepository).findByFilter(filters, USER_ID, true, pageable);
        verify(rewardTransactionRepository).getCount(filters, POS_ID, null, USER_ID, true);
    }

    @Test
    void pointOfSaleSearchAndCount_delegateProductAndStatusOrderingInputs() {
        MongoRewardTransactionSearchAdapter adapter =
                new MongoRewardTransactionSearchAdapter(rewardTransactionRepository);
        TrxFiltersDTO filters = filters();
        PageRequest pageable = PageRequest.of(0, 10);
        RewardTransaction transaction = RewardTransaction.builder().id("transaction").build();

        when(rewardTransactionRepository.findByFilterTrx(
                filters, POS_ID, USER_ID, PRODUCT_GTIN, false, pageable))
                .thenReturn(Flux.just(transaction));
        when(rewardTransactionRepository.getCount(
                filters, POS_ID, PRODUCT_GTIN, USER_ID, false))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(adapter.findPointOfSaleTransactions(
                        filters, POS_ID, USER_ID, PRODUCT_GTIN, false, pageable))
                .expectNext(transaction)
                .verifyComplete();
        StepVerifier.create(adapter.countPointOfSaleTransactions(
                        filters, POS_ID, PRODUCT_GTIN, USER_ID, false))
                .expectNext(1L)
                .verifyComplete();

        verify(rewardTransactionRepository).findByFilterTrx(
                filters, POS_ID, USER_ID, PRODUCT_GTIN, false, pageable);
        verify(rewardTransactionRepository).getCount(
                filters, POS_ID, PRODUCT_GTIN, USER_ID, false);
    }

    private TrxFiltersDTO filters() {
        return TrxFiltersDTO.builder()
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .pointOfSaleId(POS_ID)
                .trxCode("code")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .build();
    }
}
