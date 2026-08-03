package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqlMerchantRewardBatchLookupAdapterTest {

    private final SqlRewardBatchListAdapter batchListAdapter = mock(SqlRewardBatchListAdapter.class);
    private final SqlMerchantRewardBatchLookupAdapter adapter =
            new SqlMerchantRewardBatchLookupAdapter(batchListAdapter);

    @Test
    void findMerchantBatchShouldDelegateScopeAndReturnBatch() {
        RewardBatch batch = RewardBatch.builder().id("batch-id").build();
        when(batchListAdapter.findMerchantBatch("merchant-id", "initiative-id", "batch-id"))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(adapter.findMerchantBatch("merchant-id", "initiative-id", "batch-id"))
                .expectNext(batch)
                .verifyComplete();

        verify(batchListAdapter).findMerchantBatch("merchant-id", "initiative-id", "batch-id");
    }

    @Test
    void findMerchantBatchShouldPreserveEmptyResult() {
        when(batchListAdapter.findMerchantBatch("merchant-id", "initiative-id", "missing-batch"))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.findMerchantBatch("merchant-id", "initiative-id", "missing-batch"))
                .verifyComplete();

        verify(batchListAdapter).findMerchantBatch("merchant-id", "initiative-id", "missing-batch");
    }
}
