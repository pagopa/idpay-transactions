package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardBatchLifecycleAdapterTest {

    private static final String BATCH_ID = "batch";
    private static final String INITIATIVE_ID = "initiative";
    private static final String MERCHANT_ID = "merchant";

    @Mock
    private RewardBatchRepository rewardBatchRepository;

    @Mock
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Test
    void lifecycleReads_delegateToRepository() {
        MongoRewardBatchLifecycleAdapter adapter = new MongoRewardBatchLifecycleAdapter(
                rewardBatchRepository,
                reactiveMongoTemplate
        );
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).build();
        PageRequest pageable = PageRequest.of(0, 10);

        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(batch));
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.SENT
        )).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.findByStatusAndInitiativeId(RewardBatchStatus.SENT, INITIATIVE_ID))
                .thenReturn(Flux.just(batch));
        when(rewardBatchRepository.findByStatusAndInitiativeId(
                RewardBatchStatus.APPROVING, INITIATIVE_ID, pageable
        )).thenReturn(Flux.just(batch));
        when(rewardBatchRepository.findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                RewardBatchStatus.APPROVED, INITIATIVE_ID, 0L, pageable
        )).thenReturn(Flux.just(batch));
        when(rewardBatchRepository.findByMerchantIdAndInitiativeIdAndPosType(
                MERCHANT_ID, INITIATIVE_ID, PHYSICAL
        )).thenReturn(Flux.just(batch));

        StepVerifier.create(adapter.findBatch(BATCH_ID)).expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findBatch(BATCH_ID, INITIATIVE_ID)).expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.findBatchWithStatus(BATCH_ID, INITIATIVE_ID, RewardBatchStatus.SENT))
                .expectNext(batch)
                .verifyComplete();
        StepVerifier.create(adapter.findBatchesWithStatus(RewardBatchStatus.SENT, INITIATIVE_ID))
                .expectNext(batch)
                .verifyComplete();
        StepVerifier.create(adapter.findBatchesWithStatus(RewardBatchStatus.APPROVING, INITIATIVE_ID, pageable))
                .expectNext(batch)
                .verifyComplete();
        StepVerifier.create(adapter.findDeliverableBatches(INITIATIVE_ID, pageable))
                .expectNext(batch)
                .verifyComplete();
        StepVerifier.create(adapter.findMerchantBatches(MERCHANT_ID, INITIATIVE_ID, PHYSICAL))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).findById(BATCH_ID);
        verify(rewardBatchRepository).findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID);
        verify(rewardBatchRepository).findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.SENT
        );
        verify(rewardBatchRepository).findByStatusAndInitiativeId(RewardBatchStatus.SENT, INITIATIVE_ID);
        verify(rewardBatchRepository).findByStatusAndInitiativeId(
                RewardBatchStatus.APPROVING, INITIATIVE_ID, pageable
        );
        verify(rewardBatchRepository).findByStatusAndInitiativeIdAndApprovedAmountCentsGreaterThan(
                RewardBatchStatus.APPROVED, INITIATIVE_ID, 0L, pageable
        );
        verify(rewardBatchRepository).findByMerchantIdAndInitiativeIdAndPosType(
                MERCHANT_ID, INITIATIVE_ID, PHYSICAL
        );
    }

    @Test
    void lifecycleUpdates_delegateToRepository() {
        MongoRewardBatchLifecycleAdapter adapter = new MongoRewardBatchLifecycleAdapter(
                rewardBatchRepository,
                reactiveMongoTemplate
        );
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).build();

        when(rewardBatchRepository.save(batch)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.updateStatusAndApprovedAmountCents(
                BATCH_ID, RewardBatchStatus.EVALUATING, 42L, INITIATIVE_ID
        )).thenReturn(Mono.just(batch));

        StepVerifier.create(adapter.saveBatch(batch)).expectNext(batch).verifyComplete();
        StepVerifier.create(adapter.updateEvaluationStatus(BATCH_ID, INITIATIVE_ID, 42L))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).save(batch);
        verify(rewardBatchRepository).updateStatusAndApprovedAmountCents(
                BATCH_ID, RewardBatchStatus.EVALUATING, 42L, INITIATIVE_ID
        );
    }
}
