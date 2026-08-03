package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class SqlRewardBatchLifecycleAdapterTest {

    @Mock private SqlRewardBatchAdapter batchAdapter;
    @Mock private SqlRewardBatchListAdapter batchListAdapter;
    private SqlRewardBatchLifecycleAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new SqlRewardBatchLifecycleAdapter(batchAdapter, batchListAdapter);
    }

    @Test
    void delegatesScopedLifecycleReadsToTheSqlListAdapter() {
        RewardBatch batch = batch("batch", "initiative");
        PageRequest pageable = PageRequest.of(0, 10);
        when(batchListAdapter.findBatch("batch")).thenReturn(Mono.just(batch));
        when(batchListAdapter.findBatch("batch", "initiative")).thenReturn(Mono.just(batch));
        when(batchListAdapter.findBatchWithStatus("batch", "initiative", RewardBatchStatus.SENT))
                .thenReturn(Mono.just(batch));
        when(batchListAdapter.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative"))
                .thenReturn(Flux.just(batch));
        when(batchListAdapter.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative", pageable))
                .thenReturn(Flux.just(batch));
        when(batchListAdapter.findDeliverableBatches("initiative", pageable)).thenReturn(Flux.just(batch));
        when(batchListAdapter.findMerchantBatches("merchant", "initiative", PosType.PHYSICAL))
                .thenReturn(Flux.just(batch));

        StepVerifier.create(adapter.findBatch("batch")).expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findBatch("batch", "initiative")).expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findBatchWithStatus("batch", "initiative", RewardBatchStatus.SENT))
                .expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative"))
                .expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative", pageable))
                .expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findDeliverableBatches("initiative", pageable)).expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findMerchantBatches("merchant", "initiative", PosType.PHYSICAL))
                .expectNext(batch).verifyComplete();

        verify(batchListAdapter).findMerchantBatches("merchant", "initiative", PosType.PHYSICAL);
    }

    @Test
    void saveReturnsFreshAggregateOrSavedObjectWhenAggregateIsAbsent() {
        RewardBatch saved = batch("batch", "initiative");
        RewardBatch refreshed = batch("batch", "initiative");
        refreshed.setNumberOfTransactions(3L);
        when(batchAdapter.save(saved)).thenReturn(Mono.just(saved));
        when(batchListAdapter.findBatch("batch", "initiative")).thenReturn(Mono.just(refreshed));

        StepVerifier.create(adapter.saveBatch(saved)).expectNext(refreshed).verifyComplete();

        when(batchListAdapter.findBatch("batch", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(adapter.saveBatch(saved)).expectNext(saved).verifyComplete();
    }

    @Test
    void evaluationUpdateReturnsFreshAggregateOrUpdatedObjectWhenAggregateIsAbsent() {
        RewardBatch updated = batch("batch", "initiative");
        updated.setStatus(RewardBatchStatus.EVALUATING);
        RewardBatch refreshed = batch("batch", "initiative");
        refreshed.setNumberOfTransactionsElaborated(4L);
        when(batchAdapter.updateStatus("batch", "initiative", RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(updated));
        when(batchListAdapter.findBatch("batch", "initiative")).thenReturn(Mono.just(refreshed));

        StepVerifier.create(adapter.updateEvaluationStatus("batch", "initiative", 999L))
                .expectNext(refreshed).verifyComplete();
        verify(batchAdapter).updateStatus("batch", "initiative", RewardBatchStatus.EVALUATING);

        when(batchListAdapter.findBatch("batch", "initiative")).thenReturn(Mono.empty());
        StepVerifier.create(adapter.updateEvaluationStatus("batch", "initiative", 999L))
                .expectNext(updated).verifyComplete();
    }

    private static RewardBatch batch(String id, String initiativeId) {
        return RewardBatch.builder().id(id).initiativeId(initiativeId).merchantId("merchant").build();
    }
}
