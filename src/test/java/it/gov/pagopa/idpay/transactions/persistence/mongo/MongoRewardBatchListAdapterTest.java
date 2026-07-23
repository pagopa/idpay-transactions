package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardBatchListAdapterTest {

    private static final String MERCHANT_ID = "merchant";
    private static final String INITIATIVE_ID = "initiative";
    private static final String MONTH = "2025-12";

    @Mock
    private RewardBatchRepository rewardBatchRepository;

    @Test
    void findRewardBatches_delegatesAllFiltersAndPagination() {
        MongoRewardBatchListAdapter adapter = new MongoRewardBatchListAdapter(rewardBatchRepository);
        PageRequest pageable = PageRequest.of(1, 20);
        RewardBatch batch = RewardBatch.builder().id("batch").build();

        when(rewardBatchRepository.findRewardBatchesCombined(
                MERCHANT_ID,
                INITIATIVE_ID,
                RewardBatchStatus.TO_APPROVE.name(),
                "L3",
                MONTH,
                true,
                pageable
        )).thenReturn(Flux.just(batch));

        StepVerifier.create(adapter.findRewardBatches(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        RewardBatchStatus.TO_APPROVE.name(),
                        "L3",
                        MONTH,
                        true,
                        pageable
                ))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).findRewardBatchesCombined(
                MERCHANT_ID,
                INITIATIVE_ID,
                RewardBatchStatus.TO_APPROVE.name(),
                "L3",
                MONTH,
                true,
                pageable
        );
    }

    @Test
    void countRewardBatches_delegatesAuthorizationVisibleFilters() {
        MongoRewardBatchListAdapter adapter = new MongoRewardBatchListAdapter(rewardBatchRepository);

        when(rewardBatchRepository.getCountCombined(
                MERCHANT_ID,
                INITIATIVE_ID,
                RewardBatchStatus.TO_WORK.name(),
                "L1",
                MONTH,
                false
        )).thenReturn(Mono.just(3L));

        StepVerifier.create(adapter.countRewardBatches(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        RewardBatchStatus.TO_WORK.name(),
                        "L1",
                        MONTH,
                        false
                ))
                .expectNext(3L)
                .verifyComplete();

        verify(rewardBatchRepository).getCountCombined(
                MERCHANT_ID,
                INITIATIVE_ID,
                RewardBatchStatus.TO_WORK.name(),
                "L1",
                MONTH,
                false
        );
    }

    @Test
    void findBatchesBeforeMonth_delegatesGroupingFilters() {
        MongoRewardBatchListAdapter adapter = new MongoRewardBatchListAdapter(rewardBatchRepository);
        RewardBatch batch = RewardBatch.builder().id("batch").build();

        when(rewardBatchRepository.findRewardBatchByMonthBefore(
                MERCHANT_ID,
                INITIATIVE_ID,
                PHYSICAL,
                MONTH
        )).thenReturn(Flux.just(batch));

        StepVerifier.create(adapter.findBatchesBeforeMonth(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        PHYSICAL,
                        MONTH
                ))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).findRewardBatchByMonthBefore(
                MERCHANT_ID,
                INITIATIVE_ID,
                PHYSICAL,
                MONTH
        );
    }
}
