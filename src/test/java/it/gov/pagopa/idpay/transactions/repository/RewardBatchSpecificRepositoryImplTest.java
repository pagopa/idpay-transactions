package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class RewardBatchSpecificRepositoryImplTest {

    @Mock
    private ReactiveMongoTemplate mongoTemplate;

    @InjectMocks
    private RewardBatchSpecificRepositoryImpl repository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }


    @Test
    void findRewardBatchesCombined_basic() {
        RewardBatch batch = new RewardBatch();

        when(mongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Flux.just(batch));

        StepVerifier.create(repository.findRewardBatchesCombined(
                        "merchant", null, null, null, false,
                        PageRequest.of(0, 10)))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void findRewardBatchesCombined_withStatus_TO_APPROVE_noLevel() {
        RewardBatch batch = new RewardBatch();

        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.just(batch));

        StepVerifier.create(repository.findRewardBatchesCombined(
                        "merchant",
                        RewardBatchStatus.TO_APPROVE.name(),
                        null,
                        "2024-01",
                        false,
                        PageRequest.of(0, 10)))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void findRewardBatchesCombined_withStatus_TO_WORK_noLevel() {
        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchesCombined(
                        "merchant",
                        RewardBatchStatus.TO_WORK.name(),
                        null,
                        "2024-01",
                        false,
                        PageRequest.of(0, 10)))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findRewardBatchesCombined_invalidStatus_shouldReturnEmptyCriteria() {
        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.empty());

        StepVerifier.create(repository.findRewardBatchesCombined(
                        "merchant",
                        RewardBatchStatus.TO_APPROVE.name(),
                        RewardBatchAssignee.L1.name(),
                        "2024-01",
                        false,
                        PageRequest.of(0, 10)))
                .verifyComplete();
    }

    @Test
    void findRewardBatchesCombined_withOperator_true_shouldExcludeCreated() {
        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchesCombined(
                        "merchant",
                        null,
                        null,
                        null,
                        true,
                        PageRequest.of(0, 10)))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findRewardBatchesCombined_throwException() {
        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchesCombined(
                        "merchant",
                        null,
                        "levelException",
                        null,
                        true,
                        PageRequest.of(0, 10)))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void getCountCombined_shouldReturnCount() {
        when(mongoTemplate.count(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Mono.just(10L));

        StepVerifier.create(repository.getCountCombined(
                        "merchant", null, null, null, false))
                .expectNext(10L)
                .verifyComplete();
    }

    @Test
    void updateTotals_shouldUpdateAllFields() {
        BatchCountersDTO dto = BatchCountersDTO.newBatch();
        dto.incrementTrxElaborated(1L);
        dto.incrementTrxRejected(1L);
        dto.incrementTrxSuspended(1L);
        dto.incrementSuspendedAmountCents(1L);
        dto.incrementApprovedAmountCents(1L);
        dto.incrementInitialAmountCents(1L);
        dto.incrementNumberOfTransactions(1L);
        RewardBatch batch = new RewardBatch();

        when(mongoTemplate.findAndModify(any(), any(), any(), eq(RewardBatch.class)))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(repository.updateTotals("id", dto))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void updateTotals_withZeroValues_shouldOnlyUpdateDate() {
        BatchCountersDTO dto = BatchCountersDTO.newBatch();

        when(mongoTemplate.findAndModify(any(), any(), any(), eq(RewardBatch.class)))
                .thenReturn(Mono.just(new RewardBatch()));

        StepVerifier.create(repository.updateTotals("id", dto))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findRewardBatchById() {
        when(mongoTemplate.findOne(any(Query.class), eq(RewardBatch.class)))
                .thenReturn(Mono.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchById(" id "))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findRewardBatchByFilter_allParams() {
        when(mongoTemplate.findOne(any(), eq(RewardBatch.class)))
                .thenReturn(Mono.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchByFilter(
                        "id", "merchant", PosType.PHYSICAL, "2024-01"))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findRewardBatchByFilter_onlyMerchant() {
        when(mongoTemplate.findOne(any(), eq(RewardBatch.class)))
                .thenReturn(Mono.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchByFilter(
                        null, "merchant", null, null))
                .expectNextCount(1)
                .verifyComplete();
    }


    @Test
    void findRewardBatchByStatus() {
        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchByStatus(RewardBatchStatus.APPROVED))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findRewardBatchByMonthBefore() {
        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.just(new RewardBatch()));

        StepVerifier.create(repository.findRewardBatchByMonthBefore(
                        "merchant", PosType.PHYSICAL, "2024-02"))
                .expectNextCount(1)
                .verifyComplete();
    }


    @Test
    void updateStatusAndApprovedAmountCents() {
        when(mongoTemplate.findAndModify(any(), any(), any(), eq(RewardBatch.class)))
                .thenReturn(Mono.just(new RewardBatch()));

        StepVerifier.create(repository.updateStatusAndApprovedAmountCents(
                        "id",
                        RewardBatchStatus.APPROVED,
                        100L))
                .expectNextCount(1)
                .verifyComplete();
    }


    @Test
    void findPreviousEmptyBatches() {
        when(mongoTemplate.find(any(), eq(RewardBatch.class)))
                .thenReturn(Flux.just(new RewardBatch()));

        StepVerifier.create(repository.findPreviousEmptyBatches())
                .expectNextCount(1)
                .verifyComplete();
    }
}