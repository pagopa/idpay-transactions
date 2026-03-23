 package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GetRewardBatchesUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;

    private GetRewardBatchesUseCase useCase;

    @BeforeEach
    void setup() {
        useCase = new GetRewardBatchesUseCase(rewardBatchRepository);
    }

    @Test
    void execute_operatorVsMerchant() {
        PageRequest pageable = PageRequest.of(0, 2);

        RewardBatch b1 = RewardBatch.builder().id("B1").merchantId("M1").build();
        RewardBatch b2 = RewardBatch.builder().id("B2").merchantId("M2").build();

        when(rewardBatchRepository.findRewardBatchesCombined(null, null, null, null, true, pageable))
                .thenReturn(Flux.just(b1, b2));
        when(rewardBatchRepository.getCountCombined(null, null, null, null, true))
                .thenReturn(Mono.just(10L));

        StepVerifier.create(useCase.execute(null, "operator1", null, null, null, pageable))
                .assertNext(p -> {
                    assertEquals(2, p.getContent().size());
                    assertEquals(10L, p.getTotalElements());
                })
                .verifyComplete();

        when(rewardBatchRepository.findRewardBatchesCombined("M1", null, null, null, false, pageable))
                .thenReturn(Flux.just(b1));
        when(rewardBatchRepository.getCountCombined("M1", null, null, null, false))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(useCase.execute("M1", "guest", null, null, null, pageable))
                .assertNext(p -> {
                    assertEquals(1, p.getContent().size());
                    assertEquals(1L, p.getTotalElements());
                })
                .verifyComplete();
    }
}

