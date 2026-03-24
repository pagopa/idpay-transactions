package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DirtiesContext
@MongoTest
class RewardBatchSpecificRepositoryImplTest {

    public static final String MERCHANT = "merchantA";
    public static final Long ZERO_LONG = 0L;
    public static final Long ONEHUNDRED_LONG = 100L;
    public static final Long ONE_LONG = 1L;
    public static final String INITIATIVE_ID = "INIT_01";

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
        .initialAmountCents(ZERO_LONG)
        .approvedAmountCents(ZERO_LONG)
        .numberOfTransactions(ZERO_LONG)
        .numberOfTransactionsElaborated(ZERO_LONG)
        .numberOfTransactionsRejected(ZERO_LONG)
        .numberOfTransactionsSuspended(ZERO_LONG)
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
        .initialAmountCents(ZERO_LONG)
        .numberOfTransactions(ZERO_LONG)
        .numberOfTransactionsElaborated(ZERO_LONG)
        .numberOfTransactionsRejected(ZERO_LONG)
        .numberOfTransactionsSuspended(ZERO_LONG)
        .reportPath(null)
        .build();

    rewardBatchRepository.saveAll(Flux.just(batch1, batch2)).blockLast();
  }

  @AfterEach
  void clearData() {
    rewardBatchRepository.deleteAll().block();
  }


  @Test
  void findRewardBatchByMonthBefore_ShouldReturnOnlyMonthBeforeBatches() {
    batch1.setMonth("2025-11");
    batch2.setMonth("2025-12");
    batch1.setMerchantId(MERCHANT);
    batch2.setMerchantId(MERCHANT);
    batch1.setInitiativeId(INITIATIVE_ID);
    batch2.setInitiativeId(INITIATIVE_ID);
    batch1.setPosType(POS_TYPE);
    batch2.setPosType(POS_TYPE);
    String targetMonth = "2025-12";
    rewardBatchRepository.saveAll(Arrays.asList(batch1, batch2)).blockLast();

    Flux<RewardBatch> resultFlux = rewardBatchSpecificRepository.findRewardBatchByMonthBefore(MERCHANT, INITIATIVE_ID, POS_TYPE, targetMonth);

    StepVerifier.create(resultFlux)
            .expectNextCount(1)
            .verifyComplete();
  }

  @Test
  void findRewardBatchesCombined_OperatorToWorkWithoutLevel() {
    RewardBatch batchL1 = RewardBatch.builder()
            .id("BATCH_L1")
            .merchantId("M1")
            .initiativeId("INIT_01")
            .status(RewardBatchStatus.EVALUATING)
            .assigneeLevel(RewardBatchAssignee.L1)
            .build();

    RewardBatch batchL3 = RewardBatch.builder()
            .id("BATCH_L3")
            .merchantId("M1")
            .initiativeId("INIT_01")
            .status(RewardBatchStatus.EVALUATING)
            .assigneeLevel(RewardBatchAssignee.L3)
            .build();

    rewardBatchRepository.saveAll(List.of(batchL1, batchL3)).collectList().block();

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
            "M1", "INIT_01", "TO_WORK", null, null,true, PageRequest.of(0, 10));

    StepVerifier.create(result)
            .expectNextMatches(b -> b.getId().equals("BATCH_L1"))
            .verifyComplete();
  }

  @Test
  void findRewardBatchesCombined_ToApproveWithWrongLevel() {
    RewardBatch batchL3 = RewardBatch.builder()
            .id("BATCH_APPROVE")
            .initiativeId("INIT_01")
            .status(RewardBatchStatus.EVALUATING)
            .assigneeLevel(RewardBatchAssignee.L3)
            .build();
    rewardBatchRepository.save(batchL3).block();

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
            null, "INIT_01", "TO_APPROVE", "L1", null,true, PageRequest.of(0, 10));

    StepVerifier.create(result)
            .expectNextCount(0)
            .verifyComplete();
  }

  @Test
  void findRewardBatchesCombined_NonOperatorCannotSeeCreatedIfFiltered() {
    RewardBatch createdBatch = RewardBatch.builder()
            .id("CREATED_1")
            .status(RewardBatchStatus.CREATED)
            .build();
    rewardBatchRepository.save(createdBatch).block();

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
            null, null, "CREATED", null, null,true, PageRequest.of(0, 10));

    StepVerifier.create(result)
            .expectNextCount(0)
            .verifyComplete();
  }

  @Test
  void findRewardBatchesCombined_GenericFilters() {
    RewardBatch b1 = RewardBatch.builder()
            .id("B1")
            .merchantId("M1")
            .initiativeId("INIT_01")
            .month("2026-01")
            .status(RewardBatchStatus.SENT)
            .build();
    rewardBatchRepository.save(b1).block();

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
            "M1", "INIT_01", "SENT", null, "2026-01",false, PageRequest.of(0, 10));

    StepVerifier.create(result)
            .expectNextMatches(b -> b.getStatus().equals(RewardBatchStatus.SENT))
            .verifyComplete();
  }
  @Test
  void findRewardBatchByMerchantId_shouldReturnAllBatches() {
    RewardBatch batch3 = RewardBatch.builder()
        .id("batch1")
        .merchantId(MERCHANT)
        .initiativeId(INITIATIVE_ID)
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    RewardBatch batch4 = RewardBatch.builder()
        .id("batch2")
        .merchantId(MERCHANT)
        .initiativeId(INITIATIVE_ID)
        .status(RewardBatchStatus.EVALUATING)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batch3).block();
    rewardBatchRepository.save(batch4).block();

    Pageable pageable = PageRequest.of(0, 10);

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        INITIATIVE_ID,
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
        .initiativeId(INITIATIVE_ID)
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    RewardBatch batch4 = RewardBatch.builder()
        .id("B2")
        .merchantId(MERCHANT)
        .initiativeId(INITIATIVE_ID)
        .status(RewardBatchStatus.EVALUATING)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batch3).block();
    rewardBatchRepository.save(batch4).block();

    Mono<Long> countMono = rewardBatchSpecificRepository.getCountCombined(
        MERCHANT,
        INITIATIVE_ID,
        null,
        null,
        null,
        false
    );

    StepVerifier.create(countMono)
        .assertNext(count -> assertEquals(2L, count))
        .verifyComplete();
  }

  @Test
  void findRewardBatchByMerchantId_withDefaultPageable_shouldReturnSortedBatches() {
      batch1.setMerchantId(MERCHANT);
      batch2.setMerchantId(MERCHANT);
      batch1.setInitiativeId(INITIATIVE_ID);
      batch2.setInitiativeId(INITIATIVE_ID);
      batch1.setStatus(RewardBatchStatus.CREATED);
      batch2.setStatus(RewardBatchStatus.CREATED);

      rewardBatchRepository.saveAll(List.of(batch1, batch2)).blockLast();
      Pageable pageable = PageRequest.of(0, 10);

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        INITIATIVE_ID,
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
    batch1.setMerchantId(MERCHANT);
    batch2.setMerchantId(MERCHANT);
    batch1.setInitiativeId(INITIATIVE_ID);
    batch2.setInitiativeId(INITIATIVE_ID);
    batch1.setStatus(RewardBatchStatus.CREATED);
    batch2.setStatus(RewardBatchStatus.CREATED);

    rewardBatchRepository.saveAll(List.of(batch1, batch2)).blockLast();
    Pageable firstPage = PageRequest.of(0, 1, Sort.by("id").ascending());
    List<RewardBatch> page1 = rewardBatchSpecificRepository
        .findRewardBatchesCombined(
            MERCHANT,
            INITIATIVE_ID,
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
            INITIATIVE_ID,
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
  void findRewardBatchByMerchantId_withSpecificStatus_shouldFilterCorrectly() {
    RewardBatch batch3 = RewardBatch.builder()
        .id("batch3")
        .merchantId(MERCHANT)
        .initiativeId("INIT_01")
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(batch3).block();

    Pageable pageable = PageRequest.of(0, 10);

    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        INITIATIVE_ID,
        RewardBatchStatus.SENT.name(),
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
        .initiativeId("INIT_01")
        .status(RewardBatchStatus.CREATED)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batch3).block();

    Pageable pageable = PageRequest.of(0, 10);
    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchesCombined(
        MERCHANT,
        INITIATIVE_ID,
        null,
        RewardBatchAssignee.L2.name(),
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
        .initiativeId("INIT_01")
        .status(RewardBatchStatus.EVALUATING)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(batch3).block();

    Pageable pageable = PageRequest.of(0, 10);

    List<RewardBatch> result = rewardBatchSpecificRepository
        .findRewardBatchesCombined(
            null,
            INITIATIVE_ID,
            RewardBatchStatus.EVALUATING.name(),
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
        .initiativeId("INIT_01")
        .status(RewardBatchStatus.SENT)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(visibleBatch).block();

    Pageable pageable = PageRequest.of(0, 10);

    StepVerifier.create(
            rewardBatchSpecificRepository.findRewardBatchesCombined(
                null,
                    INITIATIVE_ID,
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
  void testUpdateTotals() {
    long modifiedCount = 2L;

    RewardBatch updated = rewardBatchSpecificRepository
            .updateTotals(
                    MERCHANT,
                    batch1.getId(),
                    BatchCountersDTO.newBatch()
                            .incrementTrxSuspended(modifiedCount)
            )
            .block();

    assertNotNull(updated);
    assertEquals(modifiedCount, updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertEquals(batch1.getSuspendedAmountCents(), updated.getSuspendedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }

  @Test
  void updateTotals_shouldUpdateElaboratedTrxNumber() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            MERCHANT,
            batch1.getId(),
            BatchCountersDTO.newBatch().incrementTrxElaborated(3L)
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getNumberOfTransactionsElaborated() + 3, updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getNumberOfTransactionsRejected(), updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getNumberOfTransactionsSuspended(), updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertEquals(batch1.getSuspendedAmountCents(), updated.getSuspendedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }

  @Test
  void updateTotals_shouldUpdateSuspendedTrxNumber() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            MERCHANT,
            batch1.getId(),
            BatchCountersDTO.newBatch()
                    .incrementTrxSuspended(2L)
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getNumberOfTransactionsSuspended() + 2, updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getNumberOfTransactionsRejected(), updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertEquals(batch1.getSuspendedAmountCents(), updated.getSuspendedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }

  @Test
  void updateTotals_shouldUpdateRejectedTrxNumber() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            MERCHANT,
            batch1.getId(),
            BatchCountersDTO.newBatch()
                    .incrementTrxRejected(4L)
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getNumberOfTransactionsRejected() + 4, updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getNumberOfTransactionsSuspended(), updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getApprovedAmountCents(), updated.getApprovedAmountCents());
    assertEquals(batch1.getSuspendedAmountCents(), updated.getSuspendedAmountCents());
    assertNotEquals(batch1.getUpdateDate(), updated.getUpdateDate());
  }


  @Test
  void updateTotals_shouldUpdateApprovedAmount() {
    RewardBatch updated = rewardBatchSpecificRepository.updateTotals(
            MERCHANT,
            batch1.getId(),
            BatchCountersDTO.newBatch()
                    .incrementApprovedAmountCents(500L)
    ).block();

    assertNotNull(updated);
    assertEquals(batch1.getApprovedAmountCents() + 500, updated.getApprovedAmountCents());
    assertEquals(batch1.getNumberOfTransactionsElaborated(), updated.getNumberOfTransactionsElaborated());
    assertEquals(batch1.getNumberOfTransactionsRejected(), updated.getNumberOfTransactionsRejected());
    assertEquals(batch1.getNumberOfTransactionsSuspended(), updated.getNumberOfTransactionsSuspended());
    assertEquals(batch1.getSuspendedAmountCents(), updated.getSuspendedAmountCents());
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
                    .findRewardBatchesCombined(null, null, null, null, null,true, PageRequest.of(0, 100))
                    .count()
                )
        )
        .expectNext(ONE_LONG)
        .verifyComplete();
  }

  @Test
  void findRewardBatch_withAssigneeLevel_shouldFilterByLevel() {
    RewardBatch batchL1 = RewardBatch.builder()
        .id("B1")
        .merchantId(MERCHANT)
        .initiativeId(INITIATIVE_ID)
        .status(RewardBatchStatus.APPROVED)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    RewardBatch batchL2 = RewardBatch.builder()
        .id("B2")
        .merchantId(MERCHANT)
        .initiativeId(INITIATIVE_ID)
        .status(RewardBatchStatus.APPROVED)
        .assigneeLevel(RewardBatchAssignee.L2)
        .build();

    rewardBatchRepository.save(batchL1).block();
    rewardBatchRepository.save(batchL2).block();

    Pageable pageable = PageRequest.of(0, 10);

    StepVerifier.create(
            rewardBatchSpecificRepository
                .findRewardBatchesCombined(null, INITIATIVE_ID, null, "L1", null,false, pageable)
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
        .initiativeId(INITIATIVE_ID)
        .status(RewardBatchStatus.CREATED)
        .assigneeLevel(RewardBatchAssignee.L1)
        .build();

    rewardBatchRepository.save(batchCreated).block();

    Pageable pageable = PageRequest.of(0, 10);

    List<RewardBatch> result = rewardBatchSpecificRepository
        .findRewardBatchesCombined(
            null,
            INITIATIVE_ID,
            RewardBatchStatus.CREATED.name(),
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
  void findRewardBatchByIdAndMerchantId_ShouldReturnDocument() {
    Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByIdAndMerchantId(batch1.getId(), MERCHANT);

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
            .merchantId(MERCHANT)
            .initiativeId(INITIATIVE_ID)
            .initialAmountCents(ONEHUNDRED_LONG)
            .approvedAmountCents(ZERO_LONG)
            .build();


    rewardBatchRepository.save(rewardBatch).block();

    RewardBatch resultUpdated = rewardBatchRepository
            .updateStatusAndApprovedAmountCents(rewardBatch.getId(), MERCHANT, RewardBatchStatus.EVALUATING, 200L)
            .block();

    assertNotNull(resultUpdated);
    assertEquals(RewardBatchStatus.EVALUATING, resultUpdated.getStatus());
    assertEquals(200L, resultUpdated.getApprovedAmountCents());

    rewardBatchRepository.deleteById(rewardBatch.getId()).block();

  }

    @Test
    void findRewardBatchesCombined_withNullPageable_shouldUseDefaultSortingAndSize() {
      batch1.setMerchantId(MERCHANT);
      batch2.setMerchantId(MERCHANT);
      batch1.setInitiativeId(INITIATIVE_ID);
      batch2.setInitiativeId(INITIATIVE_ID);
      batch1.setStatus(RewardBatchStatus.CREATED);
      batch2.setStatus(RewardBatchStatus.CREATED);

      rewardBatchRepository.saveAll(List.of(batch1, batch2)).blockLast();

      List<RewardBatch> result = rewardBatchSpecificRepository
                .findRewardBatchesCombined(MERCHANT, INITIATIVE_ID, null, null, null,false, null)
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void findRewardBatchesCombined_withUnsortedPageable_shouldUseDefaultMonthSort() {
        batch1.setMerchantId(MERCHANT);
        batch2.setMerchantId(MERCHANT);
        batch1.setInitiativeId(INITIATIVE_ID);
        batch2.setInitiativeId(INITIATIVE_ID);
        batch1.setStatus(RewardBatchStatus.CREATED);
        batch2.setStatus(RewardBatchStatus.CREATED);

        rewardBatchRepository.saveAll(List.of(batch1, batch2)).blockLast();

        Pageable unsorted = PageRequest.of(0, 10, Sort.unsorted());

        List<RewardBatch> result = rewardBatchSpecificRepository
                .findRewardBatchesCombined(MERCHANT, INITIATIVE_ID, null, null, null, false, unsorted)
                .collectList()
                .block();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void findRewardBatchByIdAndMerchantId_shouldTrimInput() {
        Mono<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByIdAndMerchantId("  " + batch1.getId() + "  ", MERCHANT);

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
                .approvedAmountCents(ZERO_LONG)
                .build();

        rewardBatchRepository.save(created).block();

        RewardBatch updated = rewardBatchSpecificRepository
                .updateStatusAndApprovedAmountCents(created.getId(), MERCHANT, RewardBatchStatus.APPROVED, 1234L)
                .block();

        assertNotNull(updated);
        assertEquals(RewardBatchStatus.APPROVED, updated.getStatus());
        assertEquals(1234L, updated.getApprovedAmountCents());
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
                .numberOfTransactions(ZERO_LONG)
                .status(RewardBatchStatus.SENT)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        RewardBatch oldEmpty2 = RewardBatch.builder()
                .id("old-empty-2")
                .merchantId(MERCHANT)
                .month("2000-02")
                .numberOfTransactions(ZERO_LONG)
                .status(RewardBatchStatus.SENT)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        RewardBatch oldNotEmpty = RewardBatch.builder()
                .id("old-not-empty")
                .merchantId(MERCHANT)
                .month("2000-03")
                .numberOfTransactions(ONE_LONG)
                .status(RewardBatchStatus.SENT)
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();

        RewardBatch currentEmpty = RewardBatch.builder()
                .id("current-empty")
                .merchantId(MERCHANT)
                .month(currentMonth)
                .numberOfTransactions(ZERO_LONG)
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

