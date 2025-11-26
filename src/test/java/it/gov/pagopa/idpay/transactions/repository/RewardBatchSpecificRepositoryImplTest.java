package it.gov.pagopa.idpay.transactions.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDateTime;
import java.util.List;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
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

@DirtiesContext
@MongoTest
class RewardBatchSpecificRepositoryImplTest {

    public static final String MERCHANT = "merchantA";
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
            .partial(false)
            .name("novembre 2025")
            .startDate(LocalDateTime.of(2025, 11, 1, 0, 0))
            .endDate(LocalDateTime.of(2025, 11, 30, 23, 59))
            .initialAmountCents(0L)
            .numberOfTransactions(0L)
            .numberOfTransactionsElaborated(0L)
            .numberOfTransactionsRejected(0L)
            .numberOfTransactionsSuspended(0L)
            .approvedAmountCents(0L)
            .reportPath(null)
            .build();

    batch2 = RewardBatch.builder()
            .id("batch2")
            .merchantId(MERCHANT)
            .businessName("Test business")
            .month("2025-11")
            .posType(PosType.ONLINE)
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
            .approvedAmountCents(0L)
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
    Pageable pageable = PageRequest.of(0, 10);
    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByMerchantId(MERCHANT, pageable);

    List<RewardBatch> list = result.toStream().toList();
    assertEquals(2, list.size());
    assertTrue(list.contains(batch1));
    assertTrue(list.contains(batch2));
  }

  @Test
  void getCount_shouldReturnCorrectNumber() {
    Mono<Long> countMono = rewardBatchSpecificRepository.getCount(MERCHANT);
    Long count = countMono.block();
    assertEquals(2L, count);
  }

  @Test
  void findRewardBatchByMerchantId_withDefaultPageable_shouldReturnSortedBatches() {
    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByMerchantId(MERCHANT, null);
    List<RewardBatch> list = result.toStream().toList();
    assertEquals(2, list.size());
    assertEquals("novembre 2025", list.get(0).getName());
    assertEquals("novembre 2025", list.get(1).getName());
  }

  @Test
  void findRewardBatchByMerchantId_withPagination_shouldRespectPageSize() {
    Pageable firstPage = PageRequest.of(0, 1, Sort.by("id").ascending());
    List<RewardBatch> page1 = rewardBatchSpecificRepository
        .findRewardBatchByMerchantId(MERCHANT, firstPage)
        .toStream().toList();

    assertEquals(1, page1.size());
    assertEquals("batch1", page1.get(0).getId());

    Pageable secondPage = PageRequest.of(1, 1, Sort.by("id").ascending());
    List<RewardBatch> page2 = rewardBatchSpecificRepository
        .findRewardBatchByMerchantId(MERCHANT, secondPage)
        .toStream().toList();

    assertEquals(1, page2.size());
    assertEquals("batch2", page2.get(0).getId());

    assertNotEquals(page1.get(0).getId(), page2.get(0).getId());
  }

  @Test
  void findRewardBatch_shouldReturnAllBatches() {
    Pageable pageable = PageRequest.of(0, 10);

    List<RewardBatch> result = rewardBatchSpecificRepository
        .findRewardBatch(pageable)
        .toStream()
        .toList();

    assertEquals(2, result.size());
    assertTrue(result.contains(batch1));
    assertTrue(result.contains(batch2));
  }

  @Test
  void getCountWithoutMerchant_shouldReturnTotalCount() {
    Long count = rewardBatchSpecificRepository.getCount().block();
    assertEquals(2L, count);
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

}
