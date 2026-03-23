package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EvaluatingRewardBatchesUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    private EvaluatingRewardBatchesUseCase useCase;

    @BeforeEach
    void setup() { useCase = new EvaluatingRewardBatchesUseCase(rewardBatchRepository, rewardTransactionRepository); }

    @Test
    void execute_nullList_processesAllSent() {
        RewardBatch sent = RewardBatch.builder().id("S1").status(RewardBatchStatus.SENT).initialAmountCents(100L).suspendedAmountCents(0L).build();
        when(rewardBatchRepository.findByStatus(RewardBatchStatus.SENT)).thenReturn(Flux.just(sent));
        when(rewardTransactionRepository.rewardTransactionsByBatchId("S1")).thenReturn(Mono.empty());
        when(rewardTransactionRepository.sumSuspendedAccruedRewardCents("S1")).thenReturn(Mono.just(20L));
        when(rewardBatchRepository.updateStatusAndApprovedAmountCents("S1", RewardBatchStatus.EVALUATING, 100L)).thenReturn(Mono.just(sent));
        StepVerifier.create(useCase.execute(null)).expectNext(1L).verifyComplete();
    }

    @Test
    void execute_withList_handlesMissingIdsAsEmpty() {
        when(rewardBatchRepository.findByIdAndStatus("S1", RewardBatchStatus.SENT)).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(List.of("S1"))).expectNext(0L).verifyComplete();
        verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), anyLong());
    }
}

