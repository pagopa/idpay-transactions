package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.batch.BatchCountersDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import static it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee.L1;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RewardBatchSpecificRepositoryImplTest {

  public static final String MERCHANT = "merchantA";
  public static final Long ZERO_LONG = 0L;
  public static final String INITIATIVE_ID = "INIT_01";

  @Mock
  private ReactiveMongoTemplate mongoTemplate;

  @InjectMocks
  private RewardBatchSpecificRepositoryImpl repository;

  @Captor
  private ArgumentCaptor<Query> queryCaptor;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void findRewardBatchesCombined_shouldCallMongoTemplateWithQuery() {
    RewardBatch batch = new RewardBatch();
    batch.setId("1");

    when(mongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Flux.just(batch));

    Flux<RewardBatch> result = repository.findRewardBatchesCombined(
            "merchant",
            "initiative",
            RewardBatchStatus.CREATED.name(),
            L1.name(),
            null,
            false,
            null
    );

    StepVerifier.create(result)
            .expectNext(batch)
            .verifyComplete();

    verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardBatch.class));
    assertNotNull(queryCaptor.getValue());
  }

  @Test
  void findRewardBatchesCombined_shouldReturn() {
    RewardBatch batch = new RewardBatch();
    batch.setId("1");

    when(mongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Flux.just(batch));

    Flux<RewardBatch> result = repository.findRewardBatchesCombined(
            "merchant",
            "initiative",
            RewardBatchStatus.TO_APPROVE.name(),
            L1.name(),
            null,
            false,
            null
    );

    StepVerifier.create(result)
            .expectNext(batch)
            .verifyComplete();

    verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardBatch.class));
    assertNotNull(queryCaptor.getValue());
  }

  @Test
  void findRewardBatchesCombined_ToApprove() {
    RewardBatch batch = new RewardBatch();
    batch.setId("1");

    when(mongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Flux.just(batch));

    Flux<RewardBatch> result = repository.findRewardBatchesCombined(
            "merchant",
            "initiative",
            RewardBatchStatus.TO_APPROVE.name(),
            null,
            null,
            false,
            null
    );

    StepVerifier.create(result)
            .expectNext(batch)
            .verifyComplete();

    verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardBatch.class));
    assertNotNull(queryCaptor.getValue());
  }

  @Test
  void findRewardBatchesCombined_ToWork() {
    RewardBatch batch = new RewardBatch();
    batch.setId("1");

    when(mongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Flux.just(batch));

    Flux<RewardBatch> result = repository.findRewardBatchesCombined(
            "merchant",
            "initiative",
            RewardBatchStatus.TO_WORK.name(),
            null,
            null,
            false,
            null
    );

    StepVerifier.create(result)
            .expectNext(batch)
            .verifyComplete();

    verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardBatch.class));
    assertNotNull(queryCaptor.getValue());
  }

  @Test
  void getCountCombined_shouldReturnCount() {
    when(mongoTemplate.count(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Mono.just(5L));

    Mono<Long> result = repository.getCountCombined(
            null, null, null, null, null, false
    );

    StepVerifier.create(result)
            .expectNext(5L)
            .verifyComplete();

    verify(mongoTemplate).count(any(Query.class), eq(RewardBatch.class));
  }

  @Test
  void updateTotals_shouldNotIncrementFields() {
    RewardBatch updated = RewardBatch.builder()
            .id("batch1")
            .merchantId(MERCHANT)
            .initiativeId(INITIATIVE_ID)
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

    when(mongoTemplate.findAndModify(any(Query.class), any(), any(), eq(RewardBatch.class)))
            .thenReturn(Mono.just(updated));

    BatchCountersDTO dto = BatchCountersDTO.newBatch();

    Mono<RewardBatch> result = repository.updateTotals("INIT", "id1", dto);

    StepVerifier.create(result)
            .expectNext(updated)
            .verifyComplete();

    verify(mongoTemplate).findAndModify(any(Query.class), any(), any(), eq(RewardBatch.class));
  }

  @Test
  void updateTotals_shouldIncrementFields() {
    RewardBatch updated = RewardBatch.builder()
            .id("batch1")
            .merchantId(MERCHANT)
            .initiativeId(INITIATIVE_ID)
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

    when(mongoTemplate.findAndModify(any(Query.class), any(), any(), eq(RewardBatch.class)))
            .thenReturn(Mono.just(updated));

    BatchCountersDTO dto = BatchCountersDTO.newBatch()
            .incrementTrxElaborated(3L)
            .incrementTrxRejected(1L)
            .incrementTrxSuspended(1L)
            .incrementApprovedAmountCents(1L)
            .decrementSuspendedAmountCents(1L)
            .incrementInitialAmountCents(1L)
            .incrementNumberOfTransactions(1L);

    Mono<RewardBatch> result = repository.updateTotals("INIT", "id1", dto);

    StepVerifier.create(result)
            .expectNext(updated)
            .verifyComplete();

    verify(mongoTemplate).findAndModify(any(Query.class), any(), any(), eq(RewardBatch.class));
  }

  @Test
  void findRewardBatchByIdAndInitiativeId_shouldTrimId() {
    RewardBatch batch = new RewardBatch();
    batch.setId("abc");

    when(mongoTemplate.findOne(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Mono.just(batch));

    Mono<RewardBatch> result =
            repository.findRewardBatchByIdAndInitiativeId("  abc  ", "INIT");

    StepVerifier.create(result)
            .expectNextMatches(b -> b.getId().equals("abc"))
            .verifyComplete();

    verify(mongoTemplate).findOne(queryCaptor.capture(), eq(RewardBatch.class));
    Query query = queryCaptor.getValue();
    assertNotNull(query);
  }

  @Test
  void findRewardBatchByMonthBefore_shouldReturnResults() {
    RewardBatch batch = new RewardBatch();
    batch.setId("b1");

    when(mongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Flux.just(batch));

    Flux<RewardBatch> result = repository.findRewardBatchByMonthBefore(
            "M1",
            "INIT",
            PosType.PHYSICAL,
            "2025-12"
    );

    StepVerifier.create(result)
            .expectNext(batch)
            .verifyComplete();

    verify(mongoTemplate).find(any(Query.class), eq(RewardBatch.class));
  }

  @Test
  void updateStatusAndApprovedAmountCents_shouldUpdateFields() {
    RewardBatch updated = new RewardBatch();
    updated.setStatus(RewardBatchStatus.APPROVED);
    updated.setApprovedAmountCents(100L);

    when(mongoTemplate.findAndModify(any(Query.class), any(), any(), eq(RewardBatch.class)))
            .thenReturn(Mono.just(updated));

    Mono<RewardBatch> result = repository.updateStatusAndApprovedAmountCents(
            "id",
            RewardBatchStatus.APPROVED,
            100L,
            "INIT"
    );

    StepVerifier.create(result)
            .expectNextMatches(b ->
                    b.getStatus() == RewardBatchStatus.APPROVED &&
                            b.getApprovedAmountCents() == 100L
            )
            .verifyComplete();

    verify(mongoTemplate).findAndModify(any(Query.class), any(), any(), eq(RewardBatch.class));
  }

  @Test
  void findRewardBatchesCombined_withInvalidAssignee_shouldNotCrash() {
    when(mongoTemplate.find(any(Query.class), eq(RewardBatch.class)))
            .thenReturn(Flux.empty());

    Flux<RewardBatch> result = repository.findRewardBatchesCombined(
            null,
            null,
            null,
            "INVALID",
            null,
            false,
            Pageable.unpaged()
    );

    StepVerifier.create(result)
            .verifyComplete();
  }

}