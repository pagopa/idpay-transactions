package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import it.gov.pagopa.idpay.transactions.utils.AuditUtilities;
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
class RejectTransactionsUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private ChecksErrorMapper checksErrorMapper;
    @Mock private AuditUtilities auditUtilities;
    private RejectTransactionsUseCase useCase;
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String BATCH_ID = "BATCH_ID";

    @BeforeEach
    void setup() { useCase = new RejectTransactionsUseCase(rewardBatchRepository, rewardTransactionRepository, checksErrorMapper, auditUtilities); }

    @Test
    void execute_allBranches() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();
        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("ALREADY_REJ", "APP", "TO_CHECK", "CONS", "SUSP_PREV")).reason("WHY").build();
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING)).thenReturn(Mono.just(batch));
        RewardTransaction alreadyRejected = RewardTransaction.builder().id("ALREADY_REJ").rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(10L).build())).build();
        RewardTransaction approved = RewardTransaction.builder().id("APP").rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(20L).build())).build();
        RewardTransaction toCheck = RewardTransaction.builder().id("TO_CHECK").rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(30L).build())).build();
        RewardTransaction consultable = RewardTransaction.builder().id("CONS").rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(40L).build())).build();
        RewardTransaction suspendedPrev = RewardTransaction.builder().id("SUSP_PREV").rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).rewardBatchLastMonthElaborated("2025-11").rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(50L).build())).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("ALREADY_REJ"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null))).thenReturn(Mono.just(alreadyRejected));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("APP"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null))).thenReturn(Mono.just(approved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("TO_CHECK"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null))).thenReturn(Mono.just(toCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("CONS"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null))).thenReturn(Mono.just(consultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("SUSP_PREV"), eq(RewardBatchTrxStatus.REJECTED), any(), eq(batchMonth), eq(null))).thenReturn(Mono.just(suspendedPrev));
        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), any(BatchCountersDTO.class))).thenReturn(Mono.just(updated));
        StepVerifier.create(useCase.execute(BATCH_ID, INITIATIVE_ID, req)).expectNext(updated).verifyComplete();
    }
}

