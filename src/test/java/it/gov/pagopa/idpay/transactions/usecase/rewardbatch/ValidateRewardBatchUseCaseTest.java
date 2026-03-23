package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.*;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ValidateRewardBatchUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    private ValidateRewardBatchUseCase useCase;
    private static final String BATCH_ID = "BATCH_ID";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";

    @BeforeEach
    void setup() { useCase = new ValidateRewardBatchUseCase(rewardBatchRepository); }

    @Test
    void execute_notFound() {
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute("operator1", INITIATIVE_ID, BATCH_ID)).expectError(RewardBatchNotFound.class).verify();
    }

    @Test
    void execute_L1_to_L2_success() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L1).numberOfTransactions(100L).numberOfTransactionsElaborated(20L).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.execute("operator1", INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> assertEquals(RewardBatchAssignee.L2, updated.getAssigneeLevel())).verifyComplete();
    }

    @Test
    void execute_L1_wrongRole() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L1).numberOfTransactions(100L).numberOfTransactionsElaborated(20L).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        StepVerifier.create(useCase.execute("guest", INITIATIVE_ID, BATCH_ID)).expectError(RoleNotAllowedForL1PromotionException.class).verify();
    }

    @Test
    void execute_L1_not15percent() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L1).numberOfTransactions(100L).numberOfTransactionsElaborated(10L).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        StepVerifier.create(useCase.execute("operator1", INITIATIVE_ID, BATCH_ID)).expectError(BatchNotElaborated15PercentException.class).verify();
    }

    @Test
    void execute_L2_to_L3_success() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.execute("operator2", INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> assertEquals(RewardBatchAssignee.L3, updated.getAssigneeLevel())).verifyComplete();
    }

    @Test
    void execute_L2_wrongRole() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        StepVerifier.create(useCase.execute("guest", INITIATIVE_ID, BATCH_ID)).expectError(RoleNotAllowedForL2PromotionException.class).verify();
    }

    @Test
    void execute_invalidState() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).assigneeLevel(RewardBatchAssignee.L3).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        StepVerifier.create(useCase.execute("operator3", INITIATIVE_ID, BATCH_ID)).expectError(InvalidBatchStateForPromotionException.class).verify();
    }
}

