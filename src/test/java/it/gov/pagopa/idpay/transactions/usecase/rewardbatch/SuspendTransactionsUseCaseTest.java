package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.ChecksErrorMapper;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
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
class SuspendTransactionsUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private RewardTransactionRepository rewardTransactionRepository;
    @Mock private ChecksErrorMapper checksErrorMapper;
    @Mock private AuditUtilities auditUtilities;

    private SuspendTransactionsUseCase useCase;
    private static final String INITIATIVE_ID = "INITIATIVE_ID";
    private static final String BATCH_ID = "BATCH_ID";

    @BeforeEach
    void setup() { useCase = new SuspendTransactionsUseCase(rewardBatchRepository, rewardTransactionRepository, checksErrorMapper, auditUtilities); }

    @Test
    void execute_batchNotFoundOrInvalidState() {
        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("t1")).checksError(new ChecksErrorDTO(){{ setCfError(true); }}).build();
        when(checksErrorMapper.toModel(any())).thenReturn(new ChecksError());
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING)).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(BATCH_ID, INITIATIVE_ID, req))
                .expectError(ClientExceptionWithBody.class).verify();
        verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(), any(), any(), any(), any());
    }

    @Test
    void execute_coversAllBranches() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();
        ChecksErrorDTO checks = new ChecksErrorDTO(); checks.setCfError(true);
        TransactionsRequest req = TransactionsRequest.builder()
                .transactionIds(List.of("SUSP_PREV", "APP", "TO_CHECK", "CONS", "REJ", "NULL_ACC")).reason("REASON").checksError(checks).build();
        ChecksError model = new ChecksError();
        when(checksErrorMapper.toModel(checks)).thenReturn(model);
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING)).thenReturn(Mono.just(batch));

        RewardTransaction trxSuspPrev = RewardTransaction.builder().id("SUSP_PREV").rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).rewardBatchLastMonthElaborated("2025-11").rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).build();
        RewardTransaction trxApproved = RewardTransaction.builder().id("APP").rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(200L).build())).build();
        RewardTransaction trxToCheck = RewardTransaction.builder().id("TO_CHECK").rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(300L).build())).build();
        RewardTransaction trxConsultable = RewardTransaction.builder().id("CONS").rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(400L).build())).build();
        RewardTransaction trxRejected = RewardTransaction.builder().id("REJ").rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED).rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(500L).build())).build();
        RewardTransaction trxNullAccrued = RewardTransaction.builder().id("NULL_ACC").rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).rewards(Map.of("OTHER", Reward.builder().accruedRewardCents(999L).build())).build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("SUSP_PREV"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model))).thenReturn(Mono.just(trxSuspPrev));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("APP"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model))).thenReturn(Mono.just(trxApproved));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("TO_CHECK"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model))).thenReturn(Mono.just(trxToCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("CONS"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model))).thenReturn(Mono.just(trxConsultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("REJ"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model))).thenReturn(Mono.just(trxRejected));
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("NULL_ACC"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model))).thenReturn(Mono.just(trxNullAccrued));

        RewardBatch updated = RewardBatch.builder().id(BATCH_ID).build();
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), any(BatchCountersDTO.class))).thenReturn(Mono.just(updated));

        StepVerifier.create(useCase.execute(BATCH_ID, INITIATIVE_ID, req)).expectNext(updated).verifyComplete();
        verify(auditUtilities).logTransactionsStatusChanged(eq(RewardBatchTrxStatus.SUSPENDED.name()), eq(INITIATIVE_ID), anyString(), eq(checks));
        verify(rewardBatchRepository).updateTotals(eq(BATCH_ID), any(BatchCountersDTO.class));
    }

    @Test
    void execute_alreadySuspended_sameMonth_skipsElaboratedIncrement() {
        String batchMonth = "2025-12";
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.EVALUATING).month(batchMonth).build();
        ChecksErrorDTO checks = new ChecksErrorDTO(); checks.setCfError(true);
        TransactionsRequest req = TransactionsRequest.builder().transactionIds(List.of("SUSP_SAME")).reason("R").checksError(checks).build();
        ChecksError model = new ChecksError();
        when(checksErrorMapper.toModel(checks)).thenReturn(model);
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.EVALUATING)).thenReturn(Mono.just(batch));
        RewardTransaction trxSuspSame = RewardTransaction.builder().id("SUSP_SAME").rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).rewardBatchLastMonthElaborated("2025-12").rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build())).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(BATCH_ID), eq("SUSP_SAME"), eq(RewardBatchTrxStatus.SUSPENDED), any(), eq(batchMonth), eq(model))).thenReturn(Mono.just(trxSuspSame));
        when(rewardBatchRepository.updateTotals(eq(BATCH_ID), any(BatchCountersDTO.class))).thenReturn(Mono.just(batch));
        StepVerifier.create(useCase.execute(BATCH_ID, INITIATIVE_ID, req)).expectNext(batch).verifyComplete();
    }
}

