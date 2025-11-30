package it.gov.pagopa.idpay.transactions.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@DirtiesContext
@MongoTest
class RewardBatchSpecificRepositoryImplTest {

    public static final String MERCHANT = "merchantA";
    @Autowired
  protected RewardBatchRepository rewardBatchRepository;

  @Autowired
  private RewardBatchSpecificRepositoryImpl rewardBatchSpecificRepository;

  private RewardBatch batch1;
  private RewardBatch batch2;

  @BeforeEach
  void setUp() {
    rewardBatchRepository.deleteAll().block();

    batch1 = RewardBatch.builder()
        .id("batch1")
        .merchantId(MERCHANT)
        .businessName("Test business")
        .month("2025-11")
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .assigneeLevel(RewardBatchAssignee.L1)
        .partial(false)
        .name("novembre 2025")
        .startDate(LocalDateTime.of(2025, 11, 1, 0, 0))
        .endDate(LocalDateTime.of(2025, 11, 30, 23, 59))
        .initialAmountCents(0L)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .reportPath(null)
        .build();

    batch2 = RewardBatch.builder()
        .id("batch2")
        .merchantId(MERCHANT)
        .businessName("Test business")
        .month("2025-11")
        .posType(PosType.ONLINE)
        .assigneeLevel(RewardBatchAssignee.L1)
        .status(RewardBatchStatus.CREATED)
        .partial(false)
        .name("novembre 2025")
        .startDate(LocalDateTime.of(2025, 11, 1, 0, 0))
        .endDate(LocalDateTime.of(2025, 11, 30, 23, 59))
        .initialAmountCents(0L)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .reportPath(null)
        .build();

    rewardBatchRepository.saveAll(Flux.just(batch1, batch2)).blockLast();
  }

  @AfterEach
  void clearData() {
    rewardBatchRepository.deleteAll().block();
  }

  @Test
  void findRewardBatchByMerchantId_shouldReturnAllBatches() {
    RewardBatch batch3 = RewardBatch.builder()
        .id("batch1")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    RewardBatch batch4 = RewardBatch.builder()
        .id("batch2")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.EVALUATING)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batch3).block();
    rewardBatchRepository.save(batch4).block();

    Pageable pageable = PageRequest.of(0, 10);

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        null,
        null,
        false,
        pageable
    );

    List<RewardBatch> list = result.toStream().toList();

    assertEquals(2, list.size());
    assertTrue(list.stream().anyMatch(b -> b.getId().equals("batch1")));
    assertTrue(list.stream().anyMatch(b -> b.getId().equals("batch2")));
  }

  @Test
  void getCountByMerchant_shouldReturnCorrectNumber() {
    RewardBatch batch3 = RewardBatch.builder()
        .id("B1")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    RewardBatch batch4 = RewardBatch.builder()
        .id("B2")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.EVALUATING)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batch3).block();
    rewardBatchRepository.save(batch4).block();

    Mono<Long> countMono = rewardBatchSpecificRepository.getCountCombined(
        MERCHANT,
        null,
        null,
        false
    );

    StepVerifier.create(countMono)
        .assertNext(count -> assertEquals(4L, count))
        .verifyComplete();
  }

  @Test
  void findRewardBatchByMerchantId_withDefaultPageable_shouldReturnSortedBatches() {
    Pageable pageable = PageRequest.of(0, 10);

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        null,
        null,
        false,
        pageable
    );

    List<RewardBatch> list = result.toStream().toList();
    assertEquals(2, list.size());
    assertEquals("novembre 2025", list.get(0).getName());
    assertEquals("novembre 2025", list.get(1).getName());
  }

  @Test
  void findRewardBatchByMerchantId_withPagination_shouldRespectPageSize() {
    Pageable firstPage = PageRequest.of(0, 1, Sort.by("id").ascending());
    List<RewardBatch> page1 = rewardBatchSpecificRepository
        .findRewardBatchesCombined(
            MERCHANT,
            null,
            null,
            false,
            firstPage
        )
        .toStream()
        .toList();

    assertEquals(1, page1.size());
    assertEquals("batch1", page1.getFirst().getId());

    Pageable secondPage = PageRequest.of(1, 1, Sort.by("id").ascending());
    List<RewardBatch> page2 = rewardBatchSpecificRepository
        .findRewardBatchesCombined(
            MERCHANT,
            null,
            null,
            false,
            secondPage
        )
        .toStream()
        .toList();

    assertEquals(1, page2.size());
    assertEquals("batch2", page2.getFirst().getId());

    assertNotEquals(page1.getFirst().getId(), page2.getFirst().getId());
  }

  @Test
  void incrementTotals_shouldUpdateFieldsCorrectly() {
    long increment = 500L;

    RewardBatch updated = rewardBatchSpecificRepository
        .incrementTotals(batch1.getId(), increment)
        .block();

    assertNotNull(updated);
    assertEquals(batch1.getInitialAmountCents() + increment, updated.getInitialAmountCents());
    assertEquals(batch1.getNumberOfTransactions() + 1, updated.getNumberOfTransactions());
    assertNotNull(updated.getUpdateDate());

    RewardBatch fromDb = rewardBatchRepository.findById(batch1.getId()).block();
    assertNotNull(fromDb);
    assertEquals(updated.getInitialAmountCents(), fromDb.getInitialAmountCents());
    assertEquals(updated.getNumberOfTransactions(), fromDb.getNumberOfTransactions());
    assertNotNull(fromDb.getUpdateDate());
  }

  @Test
  void decrementTotals_shouldUpdateFieldsCorrectly() {
    // Prima incrementiamo per avere valori positivi
    rewardBatchSpecificRepository
        .incrementTotals(batch1.getId(), 1000L)
        .block();

    RewardBatch batchBeforeDecrement = rewardBatchRepository.findById(batch1.getId()).block();
    assertNotNull(batchBeforeDecrement);

    long decrement = 500L;

    RewardBatch updated = rewardBatchSpecificRepository
        .decrementTotals(batch1.getId(), decrement)
        .block();

    assertNotNull(updated);
    assertEquals(batchBeforeDecrement.getInitialAmountCents() - decrement, updated.getInitialAmountCents());
    assertEquals(batchBeforeDecrement.getNumberOfTransactions() - 1, updated.getNumberOfTransactions());
    assertNotNull(updated.getUpdateDate());

    RewardBatch fromDb = rewardBatchRepository.findById(batch1.getId()).block();
    assertNotNull(fromDb);
    assertEquals(updated.getInitialAmountCents(), fromDb.getInitialAmountCents());
    assertEquals(updated.getNumberOfTransactions(), fromDb.getNumberOfTransactions());
    assertNotNull(fromDb.getUpdateDate());
  }

  @Test
  void findRewardBatchByMerchantId_withSpecificStatus_shouldFilterCorrectly() {
    RewardBatch batch3 = RewardBatch.builder()
        .id("batch3")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(batch3).block();

    Pageable pageable = PageRequest.of(0, 10);

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        RewardBatchStatus.SENT.name(),
        null,
        false,
        pageable
    );

    List<RewardBatch> list = result.toStream().toList();
    assertEquals(1, list.size());
    assertEquals("batch3", list.getFirst().getId());
    assertEquals(RewardBatchStatus.SENT, list.getFirst().getStatus());
  }

  @Test
  void findRewardBatchByMerchantId_withSpecificAssigneeLevel_shouldFilterCorrectly() {
    RewardBatch batch3 = RewardBatch.builder()
        .id("batch3")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.CREATED)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batch3).block();

    Pageable pageable = PageRequest.of(0, 10);
    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        null,
        RewardBatchAssignee.L2.name(),
        false,
        pageable
    );

    List<RewardBatch> list = result.toStream().toList();
    assertEquals(1, list.size());
    assertEquals("batch3", list.getFirst().getId());
    assertEquals(RewardBatchAssignee.L2, list.getFirst().getAssigneeLevel());
  }

  @Test
  void findRewardBatch_withSpecificStatus_shouldFilterCorrectly_status_EVALUATING() {
    RewardBatch batch3 = RewardBatch.builder()
        .id("batch3")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.EVALUATING)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(batch3).block();

    Pageable pageable = PageRequest.of(0, 10);

    List<RewardBatch> result = rewardBatchSpecificRepository
        .findRewardBatchesCombined(
            null,
            RewardBatchStatus.EVALUATING.name(),
            null,
            false,
            pageable
        )
        .toStream()
        .toList();

    assertEquals(1, result.size());
    assertEquals("batch3", result.getFirst().getId());
    assertEquals(RewardBatchStatus.EVALUATING, result.getFirst().getStatus());
  }

  @Test
  void findRewardBatch_withoutMerchant_shouldExcludeCreated() {
    RewardBatch visibleBatch = RewardBatch.builder()
        .id("batch-visible")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(visibleBatch).block();

    Pageable pageable = PageRequest.of(0, 10);

    StepVerifier.create(
            rewardBatchSpecificRepository.findRewardBatchesCombined(
                null,
                null,
                null,
                true,
                pageable
            ).collectList()
        )
        .assertNext(result -> {
          assertTrue(result.stream().noneMatch(b -> b.getStatus() == RewardBatchStatus.CREATED));
          assertTrue(result.stream().anyMatch(b -> b.getId().equals("batch-visible")));
        })
        .verifyComplete();
  }

  @Test
  void getCount_withoutMerchant_shouldExcludeCreated() {
    RewardBatch visibleBatch = RewardBatch.builder()
        .id("batch-visible")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.APPROVED)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(visibleBatch).block();

    StepVerifier.create(
            rewardBatchSpecificRepository.getCountCombined(
                    null,
                    null,
                    null,
                    true
                )
                .flatMap(count -> rewardBatchSpecificRepository
                    .findRewardBatchesCombined(null, null, null, true, PageRequest.of(0, 100))
                    .count()
                )
        )
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void findRewardBatch_withAssigneeLevel_shouldFilterByLevel() {
    RewardBatch batchL1 = RewardBatch.builder()
        .id("B1")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.APPROVED)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    RewardBatch batchL2 = RewardBatch.builder()
        .id("B2")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.APPROVED)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batchL1).block();
    rewardBatchRepository.save(batchL2).block();

    Pageable pageable = PageRequest.of(0, 10);

    StepVerifier.create(
            rewardBatchSpecificRepository
                .findRewardBatchesCombined(null, null, "L1", false, pageable)
                .collectList()
        )
        .assertNext(result -> {
          assertTrue(result.stream().allMatch(b -> b.getAssigneeLevel() == RewardBatchAssignee.L1));
          assertTrue(result.stream().anyMatch(b -> b.getId().equals("B1")));
        })
        .verifyComplete();
  }

  @Test
  void findRewardBatch_operatorRequestsCreated_shouldReturnEmpty() {
    RewardBatch batchCreated = RewardBatch.builder()
        .id("batch-created")
        .merchantId(MERCHANT)
        .status(RewardBatchStatus.CREATED)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(batchCreated).block();

    Pageable pageable = PageRequest.of(0, 10);

    List<RewardBatch> result = rewardBatchSpecificRepository
        .findRewardBatchesCombined(
            null,
            RewardBatchStatus.CREATED.name(),
            null,
            true,
            pageable
        )
        .toStream()
        .toList();

    assertTrue(result.isEmpty());
  }
}

