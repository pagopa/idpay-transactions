package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PostponeTransactionUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private RewardBatchService rewardBatchService;
    private PostponeTransactionUseCase useCase;
    private PostponeTransactionUseCase useCaseSpy;
    private static final String MERCHANT_ID = "MERCHANT_ID";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String BUSINESS_NAME = "Business";
    private static final String BATCH_ID = "BATCH_ID";
    private static final String BATCH_ID_2 = "BATCH_ID_2";

    @BeforeEach
    void setup() {
        useCase = new PostponeTransactionUseCase(rewardBatchRepository, rewardTransactionRepository, rewardBatchService);
        useCaseSpy = spy(useCase);
    }

    @Test
    void execute_transactionNotFound() {
        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1")).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.now())).expectError(ClientExceptionNoBody.class).verify();
    }

    @Test
    void execute_batchNotFound() {
        RewardTransaction trx = RewardTransaction.builder().id("T1").merchantId(MERCHANT_ID).rewardBatchId(BATCH_ID).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).build();
        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1")).thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.now())).expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void execute_invalidBatchStatus() {
        RewardTransaction trx = RewardTransaction.builder().id("T1").merchantId(MERCHANT_ID).rewardBatchId(BATCH_ID).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).build();
        RewardBatch current = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).businessName(BUSINESS_NAME).posType(PHYSICAL).month("2025-12").status(RewardBatchStatus.SENT).build();
        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1")).thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.now())).expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void execute_limitExceeded() {
        RewardTransaction trx = RewardTransaction.builder().id("T1").merchantId(MERCHANT_ID).rewardBatchId(BATCH_ID).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).build();
        RewardBatch current = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).businessName(BUSINESS_NAME).posType(PHYSICAL).month("2026-12").status(RewardBatchStatus.CREATED).build();
        LocalDate initiativeEnd = LocalDate.of(2026, 1, 6);
        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1")).thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", initiativeEnd)).expectError(ClientExceptionWithBody.class).verify();
    }

    @Test
    void execute_success_movesAndUpdatesTrx() {
        RewardTransaction trx = RewardTransaction.builder().id("T1").merchantId(MERCHANT_ID).rewardBatchId(BATCH_ID).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).build();
        RewardBatch current = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).businessName(BUSINESS_NAME).posType(PHYSICAL).month("2026-01").status(RewardBatchStatus.CREATED).build();
        RewardBatch next = RewardBatch.builder().id(BATCH_ID_2).merchantId(MERCHANT_ID).businessName(BUSINESS_NAME).posType(PHYSICAL).month("2026-02").status(RewardBatchStatus.CREATED).build();
        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "T1")).thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchService.findOrCreateBatch(MERCHANT_ID, PHYSICAL, "2026-02", BUSINESS_NAME)).thenReturn(Mono.just(next));
        when(rewardTransactionRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), any())).thenReturn(Mono.empty());
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID_2), any())).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "T1", LocalDate.of(2026, 1, 6))).verifyComplete();
        verify(rewardBatchRepository, times(2)).updateTotals(anyString(), any());
        assertEquals(BATCH_ID_2, trx.getRewardBatchId());
        assertNotNull(trx.getRewardBatchInclusionDate());
        assertNotNull(trx.getUpdateDate());
    }

    @Test
    void execute_whenTransactionIsSuspended_shouldUpdateSuspendedCounters() {
        long accruedRewardCents = 100L;
        RewardTransaction trx = new RewardTransaction(); trx.setId("TRX_ID"); trx.setRewardBatchId(BATCH_ID); trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
        Reward reward = new Reward(); reward.setAccruedRewardCents(accruedRewardCents);
        Map<String, Reward> rewardsMap = new HashMap<>(); rewardsMap.put(INITIATIVE_ID, reward); trx.setRewards(rewardsMap);
        RewardBatch currentBatch = new RewardBatch(); currentBatch.setId(BATCH_ID); currentBatch.setStatus(RewardBatchStatus.CREATED); currentBatch.setMonth("2026-01"); currentBatch.setMerchantId(MERCHANT_ID); currentBatch.setBusinessName(BUSINESS_NAME);
        RewardBatch nextBatch = new RewardBatch(); nextBatch.setId(BATCH_ID_2); nextBatch.setStatus(RewardBatchStatus.CREATED);
        when(rewardTransactionRepository.findTransactionInBatch(MERCHANT_ID, BATCH_ID, "TRX_ID")).thenReturn(Mono.just(trx));
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(currentBatch));
        when(rewardBatchService.findOrCreateBatch(any(), any(), any(), any())).thenReturn(Mono.just(nextBatch));
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), any())).thenReturn(Mono.empty());
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID_2), any())).thenReturn(Mono.empty());
        when(rewardTransactionRepository.save(any())).thenReturn(Mono.just(trx));
        LocalDate initiativeEndDate = LocalDate.of(2026, 12, 31);
        StepVerifier.create(useCase.execute(MERCHANT_ID, INITIATIVE_ID, BATCH_ID, "TRX_ID", initiativeEndDate)).verifyComplete();
        verify(rewardBatchRepository, times(2)).updateTotals(anyString(), any(BatchCountersDTO.class));
        verify(rewardTransactionRepository).save(trx);
    }
}

