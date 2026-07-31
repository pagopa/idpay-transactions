package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import it.gov.pagopa.common.web.exception.BatchNotElaborated15PercentException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardBatchAssigneePromotionAdapterTest {

    private static final String BATCH_ID = "batch";
    private static final String INITIATIVE_ID = "initiative";

    @Mock
    private RewardBatchRepository rewardBatchRepository;
    @Mock
    private RewardTransactionRepository rewardTransactionRepository;

    @Test
    void promoteL1ToL2_rejectsWhenTransactionRowsAreBelowThresholdDespiteBatchCounters() {
        RewardBatch batch = batch(RewardBatchAssignee.L1, 20L, 20L);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.findByRewardBatchIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(transactions(2, 18));

        StepVerifier.create(adapter().promote(
                        BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L1, RewardBatchAssignee.L2
                ))
                .expectError(BatchNotElaborated15PercentException.class)
                .verify();

        verify(rewardTransactionRepository).findByRewardBatchIdAndInitiativeId(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchRepository, never()).save(any(RewardBatch.class));
    }

    @Test
    void promoteL1ToL2_promotesAtTransactionDerivedThresholdDespiteBatchCounters() {
        RewardBatch batch = batch(RewardBatchAssignee.L1, 1L, 0L);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(batch));
        when(rewardTransactionRepository.findByRewardBatchIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(transactions(3, 17));
        when(rewardBatchRepository.save(batch)).thenReturn(Mono.just(batch));

        StepVerifier.create(adapter().promote(
                        BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L1, RewardBatchAssignee.L2
                ))
                .assertNext(promoted -> assertEquals(RewardBatchAssignee.L2, promoted.getAssigneeLevel()))
                .verifyComplete();

        verify(rewardTransactionRepository).findByRewardBatchIdAndInitiativeId(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchRepository).save(batch);
    }

    @Test
    void promoteL2ToL3_doesNotQueryTransactionRows() {
        RewardBatch batch = batch(RewardBatchAssignee.L2, 0L, 0L);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(batch));
        when(rewardBatchRepository.save(batch)).thenReturn(Mono.just(batch));

        StepVerifier.create(adapter().promote(
                        BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L2, RewardBatchAssignee.L3
                ))
                .assertNext(promoted -> assertEquals(RewardBatchAssignee.L3, promoted.getAssigneeLevel()))
                .verifyComplete();

        verify(rewardTransactionRepository, never())
                .findByRewardBatchIdAndInitiativeId(any(), any());
    }

    @Test
    void promote_returnsEmptyWhenBatchDoesNotHaveExpectedAssignee() {
        RewardBatch batch = batch(RewardBatchAssignee.L2, 20L, 20L);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(adapter().promote(
                        BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L1, RewardBatchAssignee.L2
                ))
                .verifyComplete();

        verify(rewardTransactionRepository, never())
                .findByRewardBatchIdAndInitiativeId(any(), any());
        verify(rewardBatchRepository, never()).save(any(RewardBatch.class));
    }

    @Test
    void promote_rejectsUnsupportedAssigneeTransitionWithoutQueryingOrSaving() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> adapter().promote(BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L1, RewardBatchAssignee.L3)
        );

        assertEquals("Unsupported reward batch assignee promotion", error.getMessage());
        verifyNoInteractions(rewardBatchRepository, rewardTransactionRepository);
    }

    @Test
    void promote_rejectsL2ToL1TransitionWithoutQueryingOrSaving() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> adapter().promote(BATCH_ID, INITIATIVE_ID, RewardBatchAssignee.L2, RewardBatchAssignee.L1)
        );

        assertEquals("Unsupported reward batch assignee promotion", error.getMessage());
        verifyNoInteractions(rewardBatchRepository, rewardTransactionRepository);
    }

    private MongoRewardBatchAssigneePromotionAdapter adapter() {
        return new MongoRewardBatchAssigneePromotionAdapter(rewardBatchRepository, rewardTransactionRepository);
    }

    private static RewardBatch batch(
            RewardBatchAssignee assignee,
            long numberOfTransactions,
            long numberOfTransactionsElaborated
    ) {
        return RewardBatch.builder()
                .id(BATCH_ID)
                .initiativeId(INITIATIVE_ID)
                .assigneeLevel(assignee)
                .numberOfTransactions(numberOfTransactions)
                .numberOfTransactionsElaborated(numberOfTransactionsElaborated)
                .build();
    }

    private static Flux<RewardTransaction> transactions(int elaborated, int pending) {
        return Flux.concat(
                Flux.just(
                                RewardBatchTrxStatus.SUSPENDED,
                                RewardBatchTrxStatus.APPROVED,
                                RewardBatchTrxStatus.REJECTED
                        )
                        .take(elaborated)
                        .map(status -> RewardTransaction.builder().rewardBatchTrxStatus(status).build()),
                Flux.range(1, pending).map(index -> RewardTransaction.builder()
                        .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE).build())
        );
    }
}
