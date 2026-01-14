package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
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

import static org.junit.jupiter.api.Assertions.*;

@DirtiesContext
@MongoTest
class RewardBatchSpecificRepositoryImplTest {

    public static final String MERCHANT = "merchantA";
  public static final PosType POS_TYPE = PosType.PHYSICAL;
    @Autowired
    protected RewardBatchRepository rewardBatchRepository;

  @Autowired
  private ReactiveMongoTemplate mongoTemplate;
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
        .approvedAmountCents(0L)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .numberOfTransactionsRejected(0L)
        .numberOfTransactionsSuspended(0L)
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
        .numberOfTransactionsRejected(0L)
        .numberOfTransactionsSuspended(0L)
        .reportPath(null)
        .build();

    rewardBatchRepository.saveAll(Flux.just(batch1, batch2)).blockLast();
  }

  @AfterEach
  void clearData() {
    rewardBatchRepository.deleteAll().block();
  }

  @Test
  void findRewardBatchByStatus_ShouldReturnOnlyApprovingBatches() {
    batch1.setStatus(RewardBatchStatus.CREATED);
    batch2.setStatus(RewardBatchStatus.APPROVING);
    RewardBatchStatus targetStatus = RewardBatchStatus.APPROVING;
    rewardBatchRepository.saveAll(Arrays.asList(batch1, batch2)).blockLast();

    Flux<RewardBatch> resultFlux = rewardBatchSpecificRepository.findRewardBatchByStatus(targetStatus);

    StepVerifier.create(resultFlux)
            .expectNextCount(1)
            .verifyComplete();
  }

  @Test
  void findRewardBatchByMonthBefore_ShouldReturnOnlyMonthBeforeBatches() {
    batch1.setMonth("2025-11");
    batch2.setMonth("2025-12");
    batch1.setMerchantId(MERCHANT);
    batch2.setMerchantId(MERCHANT);
    batch1.setPosType(POS_TYPE);
    batch2.setPosType(POS_TYPE);
    String targetMonth = "2025-12";
    rewardBatchRepository.saveAll(Arrays.asList(batch1, batch2)).blockLast();

    Flux<RewardBatch> resultFlux = rewardBatchSpecificRepository.findRewardBatchByMonthBefore(MERCHANT, POS_TYPE, targetMonth);

    StepVerifier.create(resultFlux)
            .expectNextCount(1)
            .verifyComplete();
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
        null,
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
        null,
        null,
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
            null,
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
  void testUpdateTransactionsStatus_success() {
    rewardBatchRepository.save(batch1).block();

    RewardTransaction t1 = RewardTransaction.builder()
            .id("t1")
            .rewardBatchId(batch1.getId())
            .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
            .build();

    RewardTransaction t2 = RewardTransaction.builder()
            .id("t2")
            .rewardBatchId(batch1.getId())
            .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
            .build();

    mongoTemplate.insertAll(List.of(t1, t2)).collectList().block();

    List<String> ids = List.of("t1", "t2");

    Long updated = rewardBatchSpecificRepository
            .updateTransactionsStatus(batch1.getId(), ids,
                    RewardBatchTrxStatus.SUSPENDED, "reason-test")
            .block();

    assertEquals(2L, updated);

    RewardTransaction r1 = mongoTemplate.findById("t1", RewardTransaction.class).block();
    RewardTransaction r2 = mongoTemplate.findById("t2", RewardTransaction.class).block();

    assertEquals(RewardBatchTrxStatus.SUSPENDED, r1.getRewardBatchTrxStatus());
    assertEquals("reason-test", r1.getRewardBatchRejectionReason());

    assertEquals(RewardBatchTrxStatus.SUSPENDED, r2.getRewardBatchTrxStatus());
    assertEquals("reason-test", r2.getRewardBatchRejectionReason());
  }

  @Test
  void testUpdateTransactionsStatus_partialFail() {
    RewardTransaction t1 = RewardTransaction.builder()
            .id("t1-partial")
            .rewardBatchId(batch1.getId())
            .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
            .build();

    mongoTemplate.insert(t1).block();

    List<String> ids = List.of("t1-partial", "t2-partial");

    Mono<Long> result = rewardBatchSpecificRepository.updateTransactionsStatus(
            batch1.getId(), ids,
            RewardBatchTrxStatus.SUSPENDED,
            "reason-test"
    );

    StepVerifier.create(result)
            .expectErrorMatches(err ->
                    err instanceof IllegalStateException &&
                            err.getMessage().contains("Not all transactions were updated"))
            .verify();
  }

  @Test
  void testUpdateTransactionsStatus_nullTransactionIds_returnsZero() {
    Mono<Long> result = rewardBatchSpecificRepository.updateTransactionsStatus(
            batch1.getId(),
            null,
            RewardBatchTrxStatus.SUSPENDED,
            "reason-test"
    );

    StepVerifier.create(result)
            .expectNext(0L)
            .verifyComplete();
  }

  @Test
  void testUpdateTransactionsStatus_partialUpdate_throwsException() {
    RewardTransaction existingTransaction = RewardTransaction.builder()
            .id("existing-trx")
            .rewardBatchId(batch1.getId())
            .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
            .build();

    mongoTemplate.insert(existingTransaction).block();

    List<String> transactionIds = List.of("existing-trx", "missing-trx");

    Mono<Long> result = rewardBatchSpecificRepository.updateTransactionsStatus(
            batch1.getId(),
            transactionIds,
            RewardBatchTrxStatus.SUSPENDED,
            "reason-test"
    );

    StepVerifier.create(result)
            .expectErrorMatches(err ->
                    err instanceof IllegalStateException &&
                            err.getMessage().contains("Not all transactions were updated"))
            .verify();
  }

  @Test
  void testUpdateTotals() {
    long modifiedCount = 2L;

    RewardBatch updated = rewardBatchSpecificRepository
            .updateTotals(
                    batch1.getId(),
                    0L,
                    0L,
                    0L,
                    modifiedCount
            )
            .block();

    assertNotNull(updated);
    assertEquals(modifiedCount, updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }

  @Test
  void updateTotals_shouldUpdateElaboratedTrxNumber() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            batch1.getId(),
            3L,
            0L,
            0L,
            0L
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getNumberOfTransactionsElaborated() + 3, updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getNumberOfTransactionsRejected(), updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getNumberOfTransactionsSuspended(), updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }

  @Test
  void updateTotals_shouldUpdateSuspendedTrxNumber() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            batch1.getId(),
            0L,
            0L,
            0L,
            2L
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getNumberOfTransactionsSuspended() + 2, updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getNumberOfTransactionsRejected(), updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }

  @Test
  void updateTotals_shouldUpdateRejectedTrxNumber() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            batch1.getId(),
            0L,
            0L,
            4L,
            0L
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getNumberOfTransactionsRejected() + 4, updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getNumberOfTransactionsSuspended(), updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }


  @Test
  void updateTotals_shouldUpdateApprovedAmount() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            batch1.getId(),
            0L,
            500L,
            0L,
            0L
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getApprovedAmountCents() + 500, updated.getApprovedAmountCents());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getNumberOfTransactionsRejected(), updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getNumberOfTransactionsSuspended(), updated.getNumberOfTransactionsSuspended());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
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
                    null,
                    null,
                    true
                )
                .flatMap(count -> rewardBatchSpecificRepository
                    .findRewardBatchesCombined(null, null, null, null, null, true, PageRequest.of(0, 100))
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
                .findRewardBatchesCombined(null, null, "L1", null, null, false, pageable)
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
            null,
            null,
            true,
            pageable
        )
        .toStream()
        .toList();

    assertTrue(result.isEmpty());
  }


  @Test
  void findRewardBatchById_ShouldReturnDocument() {
    Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchById(batch1.getId());

    StepVerifier.create(result)
            .expectNextMatches(batch ->
                    batch.getId().equals(batch1.getId()) &&
                            batch.getMerchantId().equals(batch1.getMerchantId()))
            .verifyComplete();
  }

  @Test
  void findRewardBatchByFilter_ShouldReturnDocument_WhenAllFiltersMatch() {
    Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByFilter(
            batch1.getId(), batch1.getMerchantId(), batch1.getPosType(), batch1.getMonth());

    StepVerifier.create(result)
            .expectNextMatches(batch ->
                    batch.getId().equals(batch1.getId()) &&
                            batch.getPosType().equals(batch1.getPosType()))
            .verifyComplete();
  }

  @Test
  void findRewardBatchByFilter_ShouldReturnEmpty_WhenFiltersDoNotMatch() {
    Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByFilter(
            null, "wrongMerchant", null, null);

    StepVerifier.create(result)
            .verifyComplete();
  }

  @Test
  void updateStatusAndApprovedAmountCents() {
    RewardBatch rewardBatch = RewardBatch.builder()
            .id("UPDATE_ID_1".trim())
            .status(RewardBatchStatus.SENT)
            .initialAmountCents(100L)
            .approvedAmountCents(0L)
            .build();


    rewardBatchRepository.save(rewardBatch).block();

    RewardBatch resultUpdated = rewardBatchRepository
            .updateStatusAndApprovedAmountCents(rewardBatch.getId(), RewardBatchStatus.EVALUATING, 200L)
            .block();

    assertNotNull(resultUpdated);
    assertEquals(RewardBatchStatus.EVALUATING, resultUpdated.getStatus());
    assertEquals(200L, resultUpdated.getApprovedAmountCents());

    rewardBatchRepository.deleteById(rewardBatch.getId()).block();

  }

    @Test
    void findRewardBatchesCombined_withNullPageable_shouldUseDefaultSortingAndSize() {
        List<RewardBatch> result = rewardBatchSpecificRepository
                .findRewardBatchesCombined(MERCHANT, null, null, null, null, false, null)
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void findRewardBatchesCombined_withUnsortedPageable_shouldUseDefaultMonthSort() {
        Pageable unsorted = PageRequest.of(0, 10, Sort.unsorted());

        List<RewardBatch> result = rewardBatchSpecificRepository
                .findRewardBatchesCombined(MERCHANT, null, null, null, null, false, unsorted)
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void findRewardBatchById_shouldTrimInput() {
        Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchById("  " + batch1.getId() + "  ");

        StepVerifier.create(result)
                .expectNextMatches(b -> b.getId().equals(batch1.getId()))
                .verifyComplete();
    }

    @Test
    void findRewardBatchByFilter_withNullBatchId_shouldFilterByMerchantPosTypeMonth() {
        Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByFilter(
                null, MERCHANT, PosType.PHYSICAL, "2025-11"
        );

        StepVerifier.create(result)
                .expectNextMatches(b ->
                        b.getMerchantId().equals(MERCHANT)
                                && b.getPosType() == PosType.PHYSICAL
                                && b.getMonth().equals("2025-11"))
                .verifyComplete();
    }

    @Test
    void findRewardBatchByFilter_withOnlyMerchant_shouldReturnOneOfMerchantBatches() {
        Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByFilter(
                null, MERCHANT, null, null
        );

        StepVerifier.create(result)
                .expectNextMatches(b -> MERCHANT.equals(b.getMerchantId()))
                .verifyComplete();
    }

    @Test
    void updateStatusAndApprovedAmountCents_shouldUpdateDocument_usingSpecificRepository() {
        RewardBatch created = RewardBatch.builder()
                .id("rb-update-1")
                .merchantId(MERCHANT)
                .status(RewardBatchStatus.SENT)
                .approvedAmountCents(0L)
                .build();

        rewardBatchRepository.save(created).block();

        RewardBatch updated = rewardBatchSpecificRepository
                .updateStatusAndApprovedAmountCents(created.getId(), RewardBatchStatus.APPROVED, 1234L)
                .block();

        assertNotNull(updated);
        assertEquals(RewardBatchStatus.APPROVED, updated.getStatus());
        assertEquals(1234L, updated.getApprovedAmountCents());
        assertNotNull(updated.getUpdateDate());
    }

    @Test
    void updateTransactionsStatus_withEmptyList_shouldReturnZeroAndNotFail() {
        Mono<Long> result = rewardBatchSpecificRepository.updateTransactionsStatus(
                batch1.getId(),
                List.of(),
                RewardBatchTrxStatus.SUSPENDED,
                "reason-test"
        );

        StepVerifier.create(result)
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void updateTotals_shouldUpdateMultipleFieldsInOneCall() {
        rewardBatchSpecificRepository.incrementTotals(batch1.getId(), 1000L).block();
        RewardBatch before = rewardBatchRepository.findById(batch1.getId()).block();
        assertNotNull(before);

        RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
                batch1.getId(),
                5L,
                700L,
                2L,
                3L
        ).block();

        assertNotNull(updated);
        assertEquals(before.getNumberOfTransactionsElaborated() + 5, updated.getNumberOfTransactionsElaborated());
        assertEquals(before.getNumberOfTransactionsRejected() + 2, updated.getNumberOfTransactionsRejected());
        assertEquals(before.getNumberOfTransactionsSuspended() + 3, updated.getNumberOfTransactionsSuspended());
        assertEquals(before.getApprovedAmountCents() + 700, updated.getApprovedAmountCents());
        assertNotNull(updated.getUpdateDate());
    }

    @Test
    void findPreviousEmptyBatches_shouldReturnOnlyEmptyBatchesBeforeCurrentMonth_sortedAsc() {
        rewardBatchRepository.deleteAll().block();

        String currentMonth = LocalDate.now()
                .withDayOfMonth(1)
                .toString()
                .substring(0, 7);

        RewardBatch oldEmpty1 = RewardBatch.builder()
                .id("old-empty-1")
                .merchantId(MERCHANT)
                .month("2000-01")
                .numberOfTransactions(0L)
                .status(RewardBatchStatus.SENT)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        RewardBatch oldEmpty2 = RewardBatch.builder()
                .id("old-empty-2")
                .merchantId(MERCHANT)
                .month("2000-02")
                .numberOfTransactions(0L)
                .status(RewardBatchStatus.SENT)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        RewardBatch oldNotEmpty = RewardBatch.builder()
                .id("old-not-empty")
                .merchantId(MERCHANT)
                .month("2000-03")
                .numberOfTransactions(1L)
                .status(RewardBatchStatus.SENT)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        RewardBatch currentEmpty = RewardBatch.builder()
                .id("current-empty")
                .merchantId(MERCHANT)
                .month(currentMonth)
                .numberOfTransactions(0L)
                .status(RewardBatchStatus.SENT)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        rewardBatchRepository.saveAll(List.of(oldEmpty2, oldEmpty1, oldNotEmpty, currentEmpty))
                .collectList()
                .block();

        List<RewardBatch> result = rewardBatchSpecificRepository.findPreviousEmptyBatches()
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("old-empty-1", result.get(0).getId());
        assertEquals("old-empty-2", result.get(1).getId());
    }



}

