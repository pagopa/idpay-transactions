package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ApproveTransactionsUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    private ApproveTransactionsUseCase useCase;
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String BATCH_ID = "BATCH_ID";

    @BeforeEach
    void setup() { useCase = new ApproveTransactionsUseCase(rewardBatchRepository, rewardTransactionRepository); }

    @Test
    void execute_allBranches() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();
        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("ALREADY_APP", "TO_CHECK", "CONS", "SUSP_PREV", "REJ")).build();
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING)).thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "ALREADY_APP", RewardBatchTrxStatus.APPROVED, null, batchMonth, null)).thenReturn(Mono.just(RewardTransaction.builder().id("ALREADY_APP").rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(10L).build())).build()));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "TO_CHECK", RewardBatchTrxStatus.APPROVED, null, batchMonth, null)).thenReturn(Mono.just(RewardTransaction.builder().id("TO_CHECK").rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(20L).build())).build()));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "CONS", RewardBatchTrxStatus.APPROVED, null, batchMonth, null)).thenReturn(Mono.just(RewardTransaction.builder().id("CONS").rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(30L).build())).build()));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "SUSP_PREV", RewardBatchTrxStatus.APPROVED, null, batchMonth, null)).thenReturn(Mono.just(RewardTransaction.builder().id("SUSP_PREV").rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).rewardBatchLastMonthElaborated("2025-11").rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(40L).build())).build()));
        when(rewardTransactionRepository.updateStatusAndReturnOld(BATCH_ID, "REJ", RewardBatchTrxStatus.APPROVED, null, batchMonth, null)).thenReturn(Mono.just(RewardTransaction.builder().id("REJ").rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build())).build()));
        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), any(BatchCountersDTO.class))).thenReturn(Mono.just(updated));
        StepVerifier.create(useCase.execute(BATCH_ID, req, INITIATIVE_ID)).expectNext(updated).verifyComplete();
    }

    @Test
    void execute_batchNotFoundOrInvalidState() {
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING)).thenReturn(Mono.empty());
        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("t1")).build();
        StepVerifier.create(useCase.execute(BATCH_ID, req, INITIATIVE_ID)).expectError(ClientExceptionWithBody.class).verify();
    }
}

