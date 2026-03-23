package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import com.mongodb.client.result.DeleteResult;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeleteEmptyRewardBatchesUseCaseTest {

    @Mock private ReactiveMongoTemplate reactiveMongoTemplate;
    private DeleteEmptyRewardBatchesUseCase useCase;

    @BeforeEach
    void setup() { useCase = new DeleteEmptyRewardBatchesUseCase(reactiveMongoTemplate); }

    @Test
    void execute_deletesMatching() {
        RewardBatch b1 = RewardBatch.builder().id("D1").month("2025-10").numberOfTransactions(0L).build();
        RewardBatch b2 = RewardBatch.builder().id("D2").month("2025-09").numberOfTransactions(0L).build();
        com.mongodb.reactivestreams.client.MongoDatabase db = mock(com.mongodb.reactivestreams.client.MongoDatabase.class);
        when(db.getName()).thenReturn("db");
        when(reactiveMongoTemplate.getMongoDatabase()).thenReturn(Mono.just(db));
        when(reactiveMongoTemplate.getCollectionName(RewardBatch.class)).thenReturn("rewardBatch");
        when(reactiveMongoTemplate.count(any(Query.class), eq(RewardBatch.class))).thenReturn(Mono.just(10L)).thenReturn(Mono.just(2L));
        when(reactiveMongoTemplate.find(any(Query.class), eq(RewardBatch.class))).thenReturn(Flux.just(b1, b2));
        when(reactiveMongoTemplate.remove(any(Query.class), eq(RewardBatch.class))).thenReturn(Mono.just(DeleteResult.acknowledged(1L)));
        StepVerifier.create(useCase.execute()).verifyComplete();
        verify(reactiveMongoTemplate, times(2)).remove(any(Query.class), eq(RewardBatch.class));
    }

    @Test
    void execute_noMatches() {
        com.mongodb.reactivestreams.client.MongoDatabase db = mock(com.mongodb.reactivestreams.client.MongoDatabase.class);
        when(db.getName()).thenReturn("db");
        when(reactiveMongoTemplate.getMongoDatabase()).thenReturn(Mono.just(db));
        when(reactiveMongoTemplate.getCollectionName(RewardBatch.class)).thenReturn("rewardBatch");
        when(reactiveMongoTemplate.count(any(Query.class), eq(RewardBatch.class))).thenReturn(Mono.just(10L)).thenReturn(Mono.just(0L));
        when(reactiveMongoTemplate.find(any(Query.class), eq(RewardBatch.class))).thenReturn(Flux.empty());
        StepVerifier.create(useCase.execute()).verifyComplete();
        verify(reactiveMongoTemplate, never()).remove(any(Query.class), eq(RewardBatch.class));
    }
}

