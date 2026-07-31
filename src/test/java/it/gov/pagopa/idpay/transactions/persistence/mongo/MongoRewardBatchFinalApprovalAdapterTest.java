package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardBatchFinalApprovalAdapterTest {

    private static final String BATCH_ID = "batch";
    private static final String INITIATIVE_ID = "initiative";

    @Mock
    private RewardBatchRepository rewardBatchRepository;
    @Mock
    private RewardTransactionRepository rewardTransactionRepository;

    @Test
    void prepareFinalApproval_approvesOnlyTransactionsEligibleForFinalApproval() {
        RewardBatch batch = batch(RewardBatchStatus.APPROVING, RewardBatchAssignee.L3);
        RewardTransaction toCheck = transaction("to-check", RewardBatchTrxStatus.TO_CHECK);
        RewardTransaction consultable = transaction("consultable", RewardBatchTrxStatus.CONSULTABLE);

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.APPROVING
        )).thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.findByFilter(
                BATCH_ID,
                INITIATIVE_ID,
                List.of(RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.CONSULTABLE)
        )).thenReturn(Flux.just(toCheck, consultable));
        when(rewardTransactionRepository.save(any(RewardTransaction.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(adapter().prepareFinalApproval(BATCH_ID, INITIATIVE_ID))
                .expectNext(batch)
                .verifyComplete();

        assertEquals(RewardBatchTrxStatus.APPROVED, toCheck.getRewardBatchTrxStatus());
        assertEquals(RewardBatchTrxStatus.APPROVED, consultable.getRewardBatchTrxStatus());
        verify(rewardTransactionRepository).findByFilter(
                BATCH_ID,
                INITIATIVE_ID,
                List.of(RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.CONSULTABLE)
        );
        verify(rewardTransactionRepository).save(toCheck);
        verify(rewardTransactionRepository).save(consultable);
    }

    @Test
    void prepareFinalApproval_andCompletionReturnEmptyForInvalidAssigneeOrState() {
        RewardBatch l2Approving = batch(RewardBatchStatus.APPROVING, RewardBatchAssignee.L2);
        RewardBatch evaluatingL3 = batch(RewardBatchStatus.EVALUATING, RewardBatchAssignee.L3);
        RewardBatch approvingL2 = batch(RewardBatchStatus.APPROVING, RewardBatchAssignee.L2);

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.APPROVING
        )).thenReturn(Mono.just(l2Approving));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(evaluatingL3), Mono.just(approvingL2));

        StepVerifier.create(adapter().prepareFinalApproval(BATCH_ID, INITIATIVE_ID)).verifyComplete();
        StepVerifier.create(adapter().completeFinalApproval(BATCH_ID, INITIATIVE_ID)).verifyComplete();
        StepVerifier.create(adapter().completeFinalApproval(BATCH_ID, INITIATIVE_ID)).verifyComplete();

        verify(rewardTransactionRepository, never()).findByFilter(
                BATCH_ID,
                INITIATIVE_ID,
                List.of(RewardBatchTrxStatus.TO_CHECK, RewardBatchTrxStatus.CONSULTABLE)
        );
        verify(rewardBatchRepository, never()).save(any());
    }

    @Test
    void completeFinalApproval_transitionsApprovingL3AndKeepsApprovedRetryIdempotent() {
        RewardBatch approving = batch(RewardBatchStatus.APPROVING, RewardBatchAssignee.L3);
        RewardBatch approved = batch(RewardBatchStatus.APPROVED, RewardBatchAssignee.L3);

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(approving), Mono.just(approved));
        when(rewardBatchRepository.save(approving)).thenReturn(Mono.just(approving));

        StepVerifier.create(adapter().completeFinalApproval(BATCH_ID, INITIATIVE_ID))
                .assertNext(result -> {
                    assertEquals(RewardBatchStatus.APPROVED, result.getStatus());
                    assertNotNull(result.getUpdateDate());
                })
                .verifyComplete();
        StepVerifier.create(adapter().completeFinalApproval(BATCH_ID, INITIATIVE_ID))
                .expectNext(approved)
                .verifyComplete();

        verify(rewardBatchRepository).save(approving);
    }

    private MongoRewardBatchFinalApprovalAdapter adapter() {
        return new MongoRewardBatchFinalApprovalAdapter(rewardBatchRepository, rewardTransactionRepository);
    }

    private static RewardBatch batch(RewardBatchStatus status, RewardBatchAssignee assignee) {
        return RewardBatch.builder()
                .id(BATCH_ID)
                .initiativeId(INITIATIVE_ID)
                .status(status)
                .assigneeLevel(assignee)
                .build();
    }

    private static RewardTransaction transaction(String id, RewardBatchTrxStatus status) {
        return RewardTransaction.builder().id(id).rewardBatchTrxStatus(status).build();
    }
}
