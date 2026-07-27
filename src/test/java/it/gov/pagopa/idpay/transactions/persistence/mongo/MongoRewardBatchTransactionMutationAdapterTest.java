package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardBatchTransactionMutationAdapterTest {

    private static final String INITIATIVE_ID = "initiative";
    private static final String SOURCE_BATCH_ID = "source";
    private static final String TARGET_BATCH_ID = "target";

    @Mock
    private RewardTransactionRepository rewardTransactionRepository;
    @Mock
    private RewardBatchRepository rewardBatchRepository;

    @Test
    void invoiceMutationsPreserveSaveThenCounterUpdateOrdering() {
        MongoRewardBatchTransactionMutationAdapter adapter = adapter();
        RewardTransaction transaction = transaction("transaction", 100L);
        RewardBatch source = batch(SOURCE_BATCH_ID);
        RewardBatch target = batch(TARGET_BATCH_ID);
        BatchCountersDTO sourceCounters = BatchCountersDTO.newBatch().decrementNumberOfTransactions();
        BatchCountersDTO targetCounters = BatchCountersDTO.newBatch().incrementNumberOfTransactions(1L);

        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));
        when(rewardBatchRepository.updateTotals(INITIATIVE_ID, SOURCE_BATCH_ID, sourceCounters))
                .thenReturn(Mono.just(source));
        when(rewardBatchRepository.updateTotals(INITIATIVE_ID, TARGET_BATCH_ID, targetCounters))
                .thenReturn(Mono.just(target));

        StepVerifier.create(adapter.moveUpdatedInvoice(
                        transaction,
                        source,
                        target,
                        sourceCounters,
                        targetCounters
                ))
                .expectNext(transaction)
                .verifyComplete();

        InOrder inOrder = inOrder(rewardTransactionRepository, rewardBatchRepository);
        inOrder.verify(rewardTransactionRepository).save(transaction);
        inOrder.verify(rewardBatchRepository).updateTotals(INITIATIVE_ID, SOURCE_BATCH_ID, sourceCounters);
        inOrder.verify(rewardBatchRepository).updateTotals(INITIATIVE_ID, TARGET_BATCH_ID, targetCounters);
    }

    @Test
    void reversalPersistsTransactionWithoutCounterUpdateWhenItHasNoBatch() {
        MongoRewardBatchTransactionMutationAdapter adapter = adapter();
        RewardTransaction transaction = transaction("transaction", 100L);
        BatchCountersDTO counters = BatchCountersDTO.newBatch().decrementNumberOfTransactions();
        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.reverseInvoice(transaction, INITIATIVE_ID, null, counters))
                .verifyComplete();

        verify(rewardTransactionRepository).save(transaction);
        verify(rewardBatchRepository, org.mockito.Mockito.never()).updateTotals(any(), any(), any());
    }

    @Test
    void suspendedReassignmentMovesMembershipAndDerivesCounterDeltas() {
        MongoRewardBatchTransactionMutationAdapter adapter = adapter();
        RewardBatch source = batch(SOURCE_BATCH_ID);
        RewardBatch target = batch(TARGET_BATCH_ID);
        RewardTransaction first = transaction("first", 100L);
        RewardTransaction second = transaction("second", null);
        second.setRewardBatchLastMonthElaborated("2025-10");

        when(rewardTransactionRepository.findByFilter(
                SOURCE_BATCH_ID,
                INITIATIVE_ID,
                List.of(RewardBatchTrxStatus.SUSPENDED)
        )).thenReturn(Flux.just(first, second));
        when(rewardTransactionRepository.save(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(TARGET_BATCH_ID), any()))
                .thenReturn(Mono.just(target));
        when(rewardBatchRepository.updateTotals(eq(INITIATIVE_ID), eq(SOURCE_BATCH_ID), any()))
                .thenReturn(Mono.just(source));

        StepVerifier.create(adapter.reassignSuspendedTransactions(
                        source,
                        target,
                        INITIATIVE_ID,
                        "2025-12"
                ))
                .verifyComplete();

        assertEquals(TARGET_BATCH_ID, first.getRewardBatchId());
        assertEquals(SyncTrxStatus.INVOICED.name(), first.getStatus());
        assertEquals("2025-12", first.getRewardBatchLastMonthElaborated());
        assertEquals(TARGET_BATCH_ID, second.getRewardBatchId());
        assertEquals("2025-10", second.getRewardBatchLastMonthElaborated());

        ArgumentCaptor<BatchCountersDTO> targetCounters = ArgumentCaptor.forClass(BatchCountersDTO.class);
        verify(rewardBatchRepository).updateTotals(eq(INITIATIVE_ID), eq(TARGET_BATCH_ID), targetCounters.capture());
        assertEquals(100L, targetCounters.getValue().getInitialAmountCents());
        assertEquals(2L, targetCounters.getValue().getNumberOfTransactions());
        assertEquals(2L, targetCounters.getValue().getTrxSuspended());
        assertEquals(2L, targetCounters.getValue().getTrxElaborated());

        ArgumentCaptor<BatchCountersDTO> sourceCounters = ArgumentCaptor.forClass(BatchCountersDTO.class);
        verify(rewardBatchRepository).updateTotals(eq(INITIATIVE_ID), eq(SOURCE_BATCH_ID), sourceCounters.capture());
        assertEquals(-2L, sourceCounters.getValue().getNumberOfTransactions());
    }

    @Test
    void postponementUpdatesCountersBeforePersistingNewMembership() {
        MongoRewardBatchTransactionMutationAdapter adapter = adapter();
        RewardTransaction transaction = transaction("transaction", 100L);
        RewardBatch source = batch(SOURCE_BATCH_ID);
        RewardBatch target = batch(TARGET_BATCH_ID);
        BatchCountersDTO sourceCounters = BatchCountersDTO.newBatch().decrementNumberOfTransactions();
        BatchCountersDTO targetCounters = BatchCountersDTO.newBatch().incrementNumberOfTransactions(1L);

        when(rewardBatchRepository.updateTotals(INITIATIVE_ID, SOURCE_BATCH_ID, sourceCounters))
                .thenReturn(Mono.just(source));
        when(rewardBatchRepository.updateTotals(INITIATIVE_ID, TARGET_BATCH_ID, targetCounters))
                .thenReturn(Mono.just(target));
        when(rewardTransactionRepository.save(transaction)).thenReturn(Mono.just(transaction));

        StepVerifier.create(adapter.postponeTransaction(
                        transaction,
                        source,
                        target,
                        sourceCounters,
                        targetCounters
                ))
                .expectNext(transaction)
                .verifyComplete();

        assertEquals(TARGET_BATCH_ID, transaction.getRewardBatchId());
        assertNotNull(transaction.getRewardBatchInclusionDate());

        InOrder inOrder = inOrder(rewardTransactionRepository, rewardBatchRepository);
        inOrder.verify(rewardBatchRepository).updateTotals(INITIATIVE_ID, SOURCE_BATCH_ID, sourceCounters);
        inOrder.verify(rewardBatchRepository).updateTotals(INITIATIVE_ID, TARGET_BATCH_ID, targetCounters);
        inOrder.verify(rewardTransactionRepository).save(transaction);
    }

    private MongoRewardBatchTransactionMutationAdapter adapter() {
        return new MongoRewardBatchTransactionMutationAdapter(
                rewardTransactionRepository,
                rewardBatchRepository
        );
    }

    private RewardBatch batch(String id) {
        return RewardBatch.builder().id(id).initiativeId(INITIATIVE_ID).build();
    }

    private RewardTransaction transaction(String id, Long accruedRewardCents) {
        return RewardTransaction.builder()
                .id(id)
                .rewards(Map.of(
                        INITIATIVE_ID,
                        Reward.builder().accruedRewardCents(accruedRewardCents).build()
                ))
                .build();
    }
}
