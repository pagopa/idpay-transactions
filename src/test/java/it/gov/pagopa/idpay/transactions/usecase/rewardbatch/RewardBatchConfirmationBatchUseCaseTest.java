package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardBatchConfirmationBatchUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private RewardBatchService rewardBatchService;
    @Mock private GenerateAndSaveCsvUseCase generateAndSaveCsvUseCase;

    private RewardBatchConfirmationBatchUseCase useCase;
    private RewardBatchConfirmationBatchUseCase useCaseSpy;

    private static final String BATCH_ID = "BATCH_ID";
    private static final String BATCH_ID_2 = "BATCH_ID_2";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String MERCHANT_ID = "MERCHANT_ID";

    @BeforeEach
    void setup() {
        useCase = new RewardBatchConfirmationBatchUseCase(rewardBatchRepository, rewardTransactionRepository, rewardBatchService, generateAndSaveCsvUseCase);
        useCaseSpy = spy(useCase);
    }

    @Test
    void execute_withIds_processesEach() {
        doReturn(Mono.just(new RewardBatch())).when(useCaseSpy).processSingleBatchConfirmation(eq(BATCH_ID), anyString());
        doReturn(Mono.just(new RewardBatch())).when(useCaseSpy).processSingleBatchConfirmation(eq(BATCH_ID_2), anyString());
        StepVerifier.create(useCaseSpy.execute(INITIATIVE_ID, List.of(BATCH_ID, BATCH_ID_2))).verifyComplete();
        verify(useCaseSpy).processSingleBatchConfirmation(BATCH_ID, INITIATIVE_ID);
        verify(useCaseSpy).processSingleBatchConfirmation(BATCH_ID_2, INITIATIVE_ID);
        verify(rewardBatchRepository, never()).findRewardBatchByStatus(any());
    }

    @Test
    void execute_emptyList_fetchesApprovingAndProcesses() {
        RewardBatch b1 = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.APPROVING).build();
        when(rewardBatchRepository.findRewardBatchByStatus(RewardBatchStatus.APPROVING)).thenReturn(Flux.just(b1));
        doReturn(Mono.just(b1)).when(useCaseSpy).processSingleBatchConfirmation(eq(BATCH_ID), anyString());
        StepVerifier.create(useCaseSpy.execute(INITIATIVE_ID, Collections.emptyList())).verifyComplete();
        verify(useCaseSpy).processSingleBatchConfirmation(BATCH_ID, INITIATIVE_ID);
    }

    @Test
    void handleSuspendedTransactions_nullOrZero_returnsOriginal() {
        RewardBatch rbNull = RewardBatch.builder().id(BATCH_ID).numberOfTransactionsSuspended(null).build();
        RewardBatch rbZero = RewardBatch.builder().id(BATCH_ID).numberOfTransactionsSuspended(0L).build();
        Mono<RewardBatch> r1 = useCase.handleSuspendedTransactions(rbNull, INITIATIVE_ID);
        Mono<RewardBatch> r2 = useCase.handleSuspendedTransactions(rbZero, INITIATIVE_ID);
        assertNotNull(r1); StepVerifier.create(r1).expectNext(rbNull).verifyComplete();
        assertNotNull(r2); StepVerifier.create(r2).expectNext(rbZero).verifyComplete();
    }

    @Test
    void updateAndSaveRewardTransactionsToApprove_setsApprovedAndSaves() {
        RewardTransaction t1 = new RewardTransaction(); t1.setId("T1"); t1.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList())).thenReturn(Flux.just(t1));
        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.updateAndSaveRewardTransactionsToApprove(BATCH_ID, INITIATIVE_ID)).verifyComplete();
        verify(rewardTransactionRepository).save(argThat(t -> t.getRewardBatchTrxStatus() == RewardBatchTrxStatus.APPROVED));
    }

    @Test
    void updateAndSaveRewardTransactionsSuspended_empty_returnsZero() {
        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList())).thenReturn(Flux.empty());
        StepVerifier.create(useCase.updateAndSaveRewardTransactionsSuspended(BATCH_ID, INITIATIVE_ID, BATCH_ID_2, "2025-12")).expectNext(0L).verifyComplete();
    }

    @Test
    void updateAndSaveRewardTransactionsSuspended_movesAndSums() {
        RewardTransaction t1 = RewardTransaction.builder().id("T1").rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).rewardBatchId(BATCH_ID).rewardBatchLastMonthElaborated(null).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).build();
        RewardTransaction t2 = RewardTransaction.builder().id("T2").rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).rewardBatchId(BATCH_ID).rewardBatchLastMonthElaborated("2025-11").rewards(null).build();
        when(rewardTransactionRepository.findByFilter(eq(BATCH_ID), eq(INITIATIVE_ID), anyList())).thenReturn(Flux.just(t1, t2));
        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.updateAndSaveRewardTransactionsSuspended(BATCH_ID, INITIATIVE_ID, BATCH_ID_2, "2025-12")).expectNext(100L).verifyComplete();
        assertEquals(BATCH_ID_2, t1.getRewardBatchId());
        assertEquals("2025-12", t1.getRewardBatchLastMonthElaborated());
        assertEquals(BATCH_ID_2, t2.getRewardBatchId());
        assertEquals("2025-11", t2.getRewardBatchLastMonthElaborated());
    }
}

