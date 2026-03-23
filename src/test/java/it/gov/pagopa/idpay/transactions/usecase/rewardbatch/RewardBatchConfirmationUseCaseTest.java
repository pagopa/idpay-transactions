package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardBatchConfirmationUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    private RewardBatchConfirmationUseCase useCase;
    private static final String BATCH_ID = "BATCH_ID";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String MERCHANT_ID = "MERCHANT_ID";

    @BeforeEach
    void setup() { useCase = new RewardBatchConfirmationUseCase(rewardBatchRepository); }

    @Test
    void execute_notFound() {
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(INITIATIVE_ID, BATCH_ID)).expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void execute_invalidState() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L2).build();
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));
        StepVerifier.create(useCase.execute(INITIATIVE_ID, BATCH_ID)).expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void execute_previousNotApproved_blocks() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L3).merchantId(MERCHANT_ID).posType(PHYSICAL).month("2025-12").build();
        RewardBatch prev = RewardBatch.builder().id("P1").status(RewardBatchStatus.SENT).build();
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));
        when(rewardBatchRepository.findRewardBatchByMonthBefore(MERCHANT_ID, PHYSICAL, "2025-12")).thenReturn(Flux.just(prev));
        StepVerifier.create(useCase.execute(INITIATIVE_ID, BATCH_ID)).expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void execute_success() {
        RewardBatch rb = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).assigneeLevel(RewardBatchAssignee.L3).merchantId(MERCHANT_ID).posType(PHYSICAL).month("2025-12").build();
        RewardBatch prevApproved = RewardBatch.builder().id("P1").status(RewardBatchStatus.APPROVED).build();
        when(rewardBatchRepository.findRewardBatchById(BATCH_ID)).thenReturn(Mono.just(rb));
        when(rewardBatchRepository.findRewardBatchByMonthBefore(MERCHANT_ID, PHYSICAL, "2025-12")).thenReturn(Flux.just(prevApproved));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.execute(INITIATIVE_ID, BATCH_ID))
                .assertNext(updated -> { assertEquals(RewardBatchStatus.APPROVING, updated.getStatus()); assertNotNull(updated.getApprovalDate()); assertNotNull(updated.getUpdateDate()); }).verifyComplete();
    }
}

