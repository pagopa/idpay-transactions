package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.RewardBatchNotFound;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchReadPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class GetRewardBatchByIdUseCaseTest {

    @Mock
    private RewardBatchReadPort rewardBatchReadPort;

    private GetRewardBatchByIdUseCase useCase;

    private static final String MERCHANT_ID = "MERCHANT_ID";
    private static final String INITIATIVE_ID = "INITIATIVE_01";
    private static final String BATCH_ID = "BATCH_ID";

    @BeforeEach
    void setup() {
        useCase = new GetRewardBatchByIdUseCase(rewardBatchReadPort);
    }

    @Test
    void execute_found() {
        RewardBatch batch = RewardBatch.builder()
                .id(BATCH_ID)
                .merchantId(MERCHANT_ID)
                .status(RewardBatchStatus.CREATED)
                .build();

        when(rewardBatchReadPort.findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchReadPort, times(1)).findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID);
    }

    @Test
    void execute_notFound() {
        when(rewardBatchReadPort.findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID))
                .expectErrorMatches(ex -> ex instanceof RewardBatchNotFound
                        && ex.getMessage().contains(BATCH_ID))
                .verify();

        verify(rewardBatchReadPort, times(1)).findByMerchantIdAndInitiativeIdAndId(MERCHANT_ID, INITIATIVE_ID, BATCH_ID);
    }
}
