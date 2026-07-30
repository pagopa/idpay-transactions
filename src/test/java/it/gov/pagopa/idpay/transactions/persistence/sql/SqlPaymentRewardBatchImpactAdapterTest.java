package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PaymentRewardBatchImpactType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.PaymentBatchEligibility;
import it.gov.pagopa.idpay.transactions.model.PaymentRewardBatchImpact;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlPaymentRewardBatchImpactAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String MERCHANT_ID = "merchant-1";
    private static final String SOURCE_BATCH_ID = "source-batch";
    private static final String TARGET_BATCH_ID = "target-batch";

    private static SqlPaymentRewardBatchImpactAdapter adapter;
    private static SqlRewardTransactionAdapter transactionAdapter;
    private static SqlInvoicedTransactionAssignmentAdapter assignmentAdapter;
    private static DSLContext dslContext;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        dslContext = DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                SQLDialect.POSTGRES
        );
        RewardTransactionSqlMapper transactionMapper = new RewardTransactionSqlMapper(JsonMapper.builder().build());
        RewardBatchSqlMapper batchMapper = new RewardBatchSqlMapper(JsonMapper.builder().build());
        transactionAdapter = new SqlRewardTransactionAdapter(
                transactionalOperator(),
                dslContext,
                transactionMapper
        );
        SqlRewardBatchAdapter batchAdapter = new SqlRewardBatchAdapter(
                transactionalOperator(),
                dslContext,
                new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                        .getRepository(RewardBatchSqlRepository.class),
                batchMapper
        );
        assignmentAdapter = new SqlInvoicedTransactionAssignmentAdapter(
                transactionalOperator(),
                dslContext,
                connectionFactory(),
                batchAdapter,
                transactionAdapter,
                batchMapper,
                transactionMapper
        );
        adapter = new SqlPaymentRewardBatchImpactAdapter(
                transactionalOperator(),
                connectionFactory(),
                dslContext,
                transactionAdapter,
                batchAdapter,
                transactionMapper,
                batchMapper
        );
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient()
                .sql("DELETE FROM reward_batch_impact_inbox")
                .fetch()
                .rowsUpdated()
                .then(databaseClient()
                        .sql("DELETE FROM reward_transactions")
                        .fetch()
                        .rowsUpdated())
                .then(databaseClient()
                        .sql("DELETE FROM reward_batches")
                        .fetch()
                        .rowsUpdated())
                .block();
    }

    @Test
    void shouldKeepCreatedMembershipAndInBatchStatusOnInvoiceReplacement() {
        String transactionId = "created-replacement";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.CREATED, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                41
                        ))
                        .then(adapter.applyImpact(replacement(transactionId, "created-event", 6L, eventTime()))))
                .assertNext(transaction -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), transaction.getStatus());
                    assertEquals(6L, transaction.getTransactionRevision());
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, transaction.getRewardBatchTrxStatus());
                    assertEquals(41, transaction.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldMoveNonCreatedMembershipToRomeEventMonthAndCreateTargetBatch() {
        String transactionId = "move-created-target";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                11
                        ))
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "move-created-target-event",
                                6L,
                                eventTime()
                        ))))
                .assertNext(transaction -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), transaction.getStatus());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED, transaction.getRewardBatchTrxStatus());
                    assertEquals(6L, transaction.getTransactionRevision());
                })
                .verifyComplete();

        StepVerifier.create(Flux.concat(
                        batchCount(),
                        batchForGrouping("2026-08"),
                        aggregate(SOURCE_BATCH_ID),
                        aggregateForGrouping("2026-08")
                ).collectList())
                .assertNext(result -> {
                    assertEquals(2L, result.get(0));
                    assertEquals(RewardBatchStatus.CREATED, ((RewardBatch) result.get(1)).getStatus());
                    assertEquals(new BatchAggregate(0L, 0L, 0L), result.get(2));
                    assertEquals(new BatchAggregate(1L, 125L, 1L), result.get(3));
                })
                .verifyComplete();
    }

    @Test
    void shouldReuseExistingOutcomeMonthGrouping() {
        String transactionId = "move-existing-target";

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07"),
                        insertBatch(TARGET_BATCH_ID, RewardBatchStatus.CREATED, "2026-08"),
                        insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.TO_CHECK,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                12
                        )
                ).then(adapter.applyImpact(replacement(
                        transactionId,
                        "move-existing-target-event",
                        6L,
                        eventTime()
                ))))
                .assertNext(transaction -> {
                    assertEquals(TARGET_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED, transaction.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(batchCount(), aggregate(TARGET_BATCH_ID)))
                .assertNext(result -> {
                    assertEquals(2L, result.getT1());
                    assertEquals(new BatchAggregate(1L, 125L, 1L), result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldDetachMembershipAndExcludeItFromTheBatchAggregateOnReversal() {
        String transactionId = "reversal";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.EVALUATING, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.INVOICED,
                                5L,
                                19
                        ))
                        .then(adapter.applyImpact(reversal(transactionId, "reversal-event", 6L))))
                .assertNext(transaction -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), transaction.getStatus());
                    assertNull(transaction.getRewardBatchId());
                    assertNull(transaction.getRewardBatchTrxStatus());
                    assertNull(transaction.getRewardBatchInclusionDate());
                    assertEquals(0, transaction.getSamplingKey());
                })
                .verifyComplete();

        StepVerifier.create(aggregate(SOURCE_BATCH_ID))
                .expectNext(new BatchAggregate(0L, 0L, 0L))
                .verifyComplete();
    }

    @Test
    void shouldApplyDuplicateImpactEventOnlyOnce() {
        String transactionId = "duplicate";
        PaymentRewardBatchImpact impact = replacement(transactionId, "duplicate-event", 6L, eventTime());

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                10
                        ))
                        .then(adapter.applyImpact(impact))
                        .then(adapter.applyImpact(impact)))
                .assertNext(transaction -> {
                    assertEquals(RewardBatchTrxStatus.SUSPENDED, transaction.getRewardBatchTrxStatus());
                    assertEquals(6L, transaction.getTransactionRevision());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(inboxCount(), batchCount()))
                .assertNext(result -> {
                    assertEquals(1L, result.getT1());
                    assertEquals(2L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectEnvelopeAndCanonicalRevisionMismatchWithoutMutatingSourceOrInbox() {
        String transactionId = "revision-mismatch";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                10
                        ))
                        .then(adapter.applyImpact(new PaymentRewardBatchImpact(
                                "revision-mismatch-event",
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                generic(transactionId, SyncTrxStatus.INVOICED, 5L)
                        ))))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains("revisions do not match"))
                .verify();

        StepVerifier.create(Mono.zip(transactionState(transactionId), inboxCount()))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            10
                    ), result.getT1());
                    assertEquals(0L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectEventIdReuseWithDifferentTransactionWithoutChangingSecondSource() {
        String firstTransactionId = "event-id-first";
        String secondTransactionId = "event-id-second";
        String secondSourceBatchId = "event-id-second-source";

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07"),
                        insertBatch(secondSourceBatchId, RewardBatchStatus.SENT, "2026-06"),
                        insertMembership(
                                firstTransactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                10
                        ),
                        insertMembership(
                                secondTransactionId,
                                secondSourceBatchId,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                11
                        )
                ).then(adapter.applyImpact(replacement(
                        firstTransactionId,
                        "reused-event-id",
                        6L,
                        eventTime()
                ))).then(adapter.applyImpact(replacement(
                        secondTransactionId,
                        "reused-event-id",
                        6L,
                        eventTime()
                ))))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("event ID reused"))
                .verify();

        StepVerifier.create(Mono.zip(transactionState(secondTransactionId), inboxCount()))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            secondSourceBatchId,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            11
                    ), result.getT1());
                    assertEquals(1L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldCompleteConcurrentOpposingReplacementsWithoutDuplicateBatches() {
        String firstSourceBatchId = "opposing-source-july";
        String secondSourceBatchId = "opposing-source-august";
        String firstTransactionId = "opposing-first";
        String secondTransactionId = "opposing-second";

        StepVerifier.create(Flux.concat(
                        insertBatch(firstSourceBatchId, RewardBatchStatus.SENT, "2026-07"),
                        insertBatch(secondSourceBatchId, RewardBatchStatus.SENT, "2026-08"),
                        insertMembership(
                                firstTransactionId,
                                firstSourceBatchId,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                10
                        ),
                        insertMembership(
                                secondTransactionId,
                                secondSourceBatchId,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                11
                        )
                ).thenMany(Flux.merge(
                        Mono.defer(() -> adapter.applyImpact(replacement(
                                firstTransactionId,
                                "opposing-first-event",
                                6L,
                                eventTime()
                        ))),
                        Mono.defer(() -> adapter.applyImpact(replacement(
                                secondTransactionId,
                                "opposing-second-event",
                                6L,
                                julyEventTime()
                        )))
                )).collectList())
                .assertNext(transactions -> {
                    assertEquals(2, transactions.size());
                    assertEquals(2, transactions.stream()
                            .map(RewardTransaction::getRewardBatchId)
                            .distinct()
                            .count());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        transactionState(firstTransactionId),
                        transactionState(secondTransactionId),
                        inboxCount(),
                        batchCount()
                ))
                .assertNext(result -> {
                    assertEquals(secondSourceBatchId, result.getT1().batchId());
                    assertEquals(firstSourceBatchId, result.getT2().batchId());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED.name(), result.getT1().batchStatus());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED.name(), result.getT2().batchStatus());
                    assertEquals(2L, result.getT3());
                    assertEquals(2L, result.getT4());
                })
                .verifyComplete();
    }

    @Test
    void shouldKeepCanonicalImpactProjectionWhenGenericSnapshotHasTheSameRevision() {
        String transactionId = "impact-before-generic";

        StepVerifier.create(adapter.applyImpact(replacement(
                                transactionId,
                                "impact-before-generic-event",
                                6L,
                                eventTime()
                        ))
                        .then(transactionAdapter.upsert(generic(
                                transactionId,
                                SyncTrxStatus.AUTHORIZED,
                                6L
                        ))))
                .assertNext(transaction -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), transaction.getStatus());
                    assertEquals(6L, transaction.getTransactionRevision());
                    assertNull(transaction.getRewardBatchId());
                })
                .verifyComplete();
    }

    @Test
    void shouldLetCanonicalImpactsWinWhenGenericSnapshotsArriveFirstAtTheSameRevision() {
        String replacementTransactionId = "generic-before-replacement";
        String reversalTransactionId = "generic-before-reversal";

        StepVerifier.create(Flux.concat(
                        transactionAdapter.upsert(generic(
                                replacementTransactionId,
                                SyncTrxStatus.AUTHORIZED,
                                6L
                        )),
                        transactionAdapter.upsert(generic(
                                reversalTransactionId,
                                SyncTrxStatus.INVOICED,
                                6L
                        ))
                ).then(adapter.applyImpact(replacement(
                        replacementTransactionId,
                        "generic-before-replacement-event",
                        6L,
                        eventTime()
                ))).then(adapter.applyImpact(reversal(
                        reversalTransactionId,
                        "generic-before-reversal-event",
                        6L
                ))))
                .assertNext(transaction -> assertEquals(SyncTrxStatus.REFUNDED.name(), transaction.getStatus()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        transactionState(replacementTransactionId),
                        transactionState(reversalTransactionId)
                ))
                .assertNext(result -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), result.getT1().status());
                    assertEquals(SyncTrxStatus.REFUNDED.name(), result.getT2().status());
                    assertEquals(6L, result.getT1().revision());
                    assertEquals(6L, result.getT2().revision());
                })
                .verifyComplete();
    }

    @Test
    void shouldSuspendInPlaceWhenNonCreatedSourceAlreadyMatchesOutcomeMonth() {
        String transactionId = "same-month-in-place";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-08")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                13
                        ))
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "same-month-in-place-event",
                                6L,
                                eventTime()
                        ))))
                .assertNext(transaction -> {
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED, transaction.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(batchCount(), aggregate(SOURCE_BATCH_ID)))
                .assertNext(result -> {
                    assertEquals(1L, result.getT1());
                    assertEquals(new BatchAggregate(1L, 125L, 1L), result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldPropagateCanonicalFieldsFromNewerImpact() {
        String transactionId = "canonical-fields";
        RewardTransaction older = generic(transactionId, SyncTrxStatus.AUTHORIZED, 5L);
        older.setFranchiseName("Old franchise");
        older.setPointOfSaleType(PosType.ONLINE);
        older.setBusinessName("Old business");
        older.setInvoiceUploadDate(LocalDateTime.of(2026, 7, 1, 9, 15));
        RewardTransaction canonical = generic(transactionId, SyncTrxStatus.INVOICED, 6L);
        canonical.setFranchiseName("Updated franchise");
        canonical.setPointOfSaleType(PosType.PHYSICAL);
        canonical.setBusinessName("Updated business");
        canonical.setInvoiceUploadDate(LocalDateTime.of(2026, 8, 1, 9, 15));

        StepVerifier.create(transactionAdapter.upsert(older)
                        .then(adapter.applyImpact(new PaymentRewardBatchImpact(
                                "canonical-fields-event",
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                canonical
                        ))))
                .assertNext(transaction -> {
                    assertEquals("Updated franchise", transaction.getFranchiseName());
                    assertEquals(PosType.PHYSICAL, transaction.getPointOfSaleType());
                    assertEquals("Updated business", transaction.getBusinessName());
                    assertEquals(LocalDateTime.of(2026, 8, 1, 9, 15), transaction.getInvoiceUploadDate());
                })
                .verifyComplete();
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("localOnlyImpactFields")
    void shouldRejectImpactContainingLocalOnlyFieldsBeforeMutatingInboxOrTransaction(
            String field,
            Consumer<RewardTransaction> localField
    ) {
        String transactionId = "local-only-" + field;
        RewardTransaction invalid = generic(transactionId, SyncTrxStatus.INVOICED, 6L);
        localField.accept(invalid);

        StepVerifier.create(transactionAdapter.upsert(generic(
                                transactionId,
                                SyncTrxStatus.AUTHORIZED,
                                5L
                        ))
                        .then(adapter.applyImpact(new PaymentRewardBatchImpact(
                                "local-only-" + field + "-event",
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                invalid
                        ))))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains("local batch membership"))
                .verify();

        StepVerifier.create(Mono.zip(transactionState(transactionId), inboxCount()))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            null,
                            null,
                            0
                    ), result.getT1());
                    assertEquals(0L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldIgnoreStaleGenericSnapshotAfterLaterImpact() {
        String transactionId = "stale-after-impact";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.INVOICED,
                                5L,
                                7
                        ))
                        .then(adapter.applyImpact(reversal(transactionId, "later-reversal", 7L)))
                        .then(transactionAdapter.upsert(generic(
                                transactionId,
                                SyncTrxStatus.INVOICED,
                                6L
                        ))))
                .assertNext(transaction -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), transaction.getStatus());
                    assertEquals(7L, transaction.getTransactionRevision());
                    assertNull(transaction.getRewardBatchId());
                    assertNull(transaction.getRewardBatchTrxStatus());
                })
                .verifyComplete();
    }

    @Test
    void shouldPropagateInitiativeMismatchWithoutChangingTheSourceTransaction() {
        String transactionId = "initiative-mismatch";
        RewardTransaction conflicting = generic(transactionId, SyncTrxStatus.INVOICED, 6L);
        conflicting.setInitiatives(List.of("other-initiative"));

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                7
                        ))
                        .then(adapter.applyImpact(new PaymentRewardBatchImpact(
                                "initiative-mismatch-event",
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                conflicting
                        ))))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains(INITIATIVE_ID))
                .verify();

        StepVerifier.create(Mono.zip(transactionState(transactionId), inboxCount()))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            7
                    ), result.getT1());
                    assertEquals(0L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectCanonicalMerchantMismatchWithoutMutatingSourceOrInbox() {
        String transactionId = "merchant-mismatch";
        RewardTransaction conflicting = generic(transactionId, SyncTrxStatus.INVOICED, 6L);
        conflicting.setMerchantId("other-merchant");

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                7
                        ))
                        .then(adapter.applyImpact(new PaymentRewardBatchImpact(
                                "merchant-mismatch-event",
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                conflicting
                        ))))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("source batch merchant " + MERCHANT_ID))
                .verify();

        StepVerifier.create(Mono.zip(transactionState(transactionId), transactionMerchant(transactionId), inboxCount()))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            7
                    ), result.getT1());
                    assertEquals(MERCHANT_ID, result.getT2());
                    assertEquals(0L, result.getT3());
                })
                .verifyComplete();
    }

    @Test
    void shouldPreserveAValidUnassignedProjectionWhenAssignmentRacesWithReversal() {
        String transactionId = "concurrent-reversal";
        RewardTransaction generic = generic(transactionId, SyncTrxStatus.INVOICED, 5L);
        RewardBatch candidate = RewardBatchFactory.create(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07",
                "Business"
        );

        StepVerifier.create(transactionAdapter.upsert(generic)
                        .thenMany(Flux.merge(
                                Mono.defer(() -> assignmentAdapter.assignInvoicedTransaction(generic, candidate, 31)),
                                Mono.defer(() -> adapter.applyImpact(reversal(
                                        transactionId,
                                        "concurrent-reversal-event",
                                        6L
                                )))
                        ))
                        .then())
                .verifyComplete();

        StepVerifier.create(transactionState(transactionId))
                .expectNext(new TransactionState(
                        SyncTrxStatus.REFUNDED.name(),
                        6L,
                        null,
                        null,
                        0
                ))
                .verifyComplete();
    }

    @Test
    void shouldReturnPaymentEligibilityOnlyForTheCurrentMerchantMembership() {
        String transactionId = "eligible";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.EVALUATING, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.SUSPENDED,
                                SyncTrxStatus.INVOICED,
                                5L,
                                4
                        ))
                        .then(adapter.findEligibility(MERCHANT_ID, transactionId)))
                .expectNext(new PaymentBatchEligibility(
                        transactionId,
                        INITIATIVE_ID,
                        MERCHANT_ID,
                        SOURCE_BATCH_ID,
                        SyncTrxStatus.INVOICED.name(),
                        RewardBatchStatus.EVALUATING,
                        RewardBatchTrxStatus.SUSPENDED
                ))
                .verifyComplete();

        StepVerifier.create(adapter.findEligibility("other-merchant", transactionId))
                .verifyComplete();
    }

    private static OffsetDateTime eventTime() {
        return OffsetDateTime.of(2026, 7, 31, 22, 30, 0, 0, ZoneOffset.UTC);
    }

    private static OffsetDateTime julyEventTime() {
        return OffsetDateTime.of(2026, 7, 1, 10, 30, 0, 0, ZoneOffset.UTC);
    }

    private static Stream<Arguments> localOnlyImpactFields() {
        return Stream.of(
                Arguments.of("sampling-key", (Consumer<RewardTransaction>)
                        transaction -> transaction.setSamplingKey(1)),
                Arguments.of("batch-rejection-reason", (Consumer<RewardTransaction>)
                        transaction -> transaction.setRewardBatchRejectionReason(List.of(new ReasonDTO(null, "local")))),
                Arguments.of("last-elaborated-month", (Consumer<RewardTransaction>)
                        transaction -> transaction.setRewardBatchLastMonthElaborated("2026-07")),
                Arguments.of("checks-error", (Consumer<RewardTransaction>)
                        transaction -> transaction.setChecksError(
                                new ChecksError(true, false, false, false, false, false, false, false)
                        ))
        );
    }

    private static PaymentRewardBatchImpact replacement(
            String transactionId,
            String eventId,
            long revision,
            OffsetDateTime occurredAt
    ) {
        return new PaymentRewardBatchImpact(
                eventId,
                1,
                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                occurredAt,
                revision,
                generic(transactionId, SyncTrxStatus.INVOICED, revision)
        );
    }

    private static PaymentRewardBatchImpact reversal(String transactionId, String eventId, long revision) {
        return new PaymentRewardBatchImpact(
                eventId,
                1,
                PaymentRewardBatchImpactType.INVOICED_REVERSED,
                eventTime(),
                revision,
                generic(transactionId, SyncTrxStatus.REFUNDED, revision)
        );
    }

    private static RewardTransaction generic(String transactionId, SyncTrxStatus status, long revision) {
        return RewardTransaction.builder()
                .id(transactionId)
                .transactionRevision(revision)
                .initiatives(List.of(INITIATIVE_ID))
                .merchantId(MERCHANT_ID)
                .pointOfSaleId("pos-1")
                .pointOfSaleType(PosType.PHYSICAL)
                .posType(PosType.PHYSICAL.name())
                .businessName("Business")
                .status(status.name())
                .trxChargeDate(LocalDateTime.of(2026, 7, 1, 10, 30))
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(125L).build()))
                .build();
    }

    private static Mono<Void> insertBatch(String batchId, RewardBatchStatus status, String month) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level
                        )
                        VALUES (
                            :id, :initiativeId, :merchantId, :month, 'PHYSICAL', :status, 'Batch', 'L1'
                        )
                        """)
                .bind("id", batchId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("merchantId", MERCHANT_ID)
                .bind("month", month)
                .bind("status", status.name())
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> insertMembership(
            String transactionId,
            String batchId,
            RewardBatchTrxStatus batchStatus,
            SyncTrxStatus status,
            long revision,
            int samplingKey
    ) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, merchant_id, point_of_sale_id, pos_type,
                            point_of_sale_type, business_name, reward_batch_id, reward_batch_trx_status,
                            reward_batch_inclusion_date, sampling_key, status, accrued_reward_cents,
                            transaction_revision
                        )
                        VALUES (
                            :transactionId, :initiativeId, :merchantId, 'pos-1', 'PHYSICAL',
                            'PHYSICAL', 'Business', :batchId, :batchStatus,
                            TIMESTAMP '2026-07-01 10:00:00', :samplingKey, :status, 125, :revision
                        )
                        """)
                .bind("transactionId", transactionId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("merchantId", MERCHANT_ID)
                .bind("batchId", batchId)
                .bind("batchStatus", batchStatus.name())
                .bind("samplingKey", samplingKey)
                .bind("status", status.name())
                .bind("revision", revision)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Long> batchCount() {
        return databaseClient()
                .sql("SELECT COUNT(*) AS count FROM reward_batches")
                .map((row, metadata) -> row.get("count", Long.class))
                .one();
    }

    private static Mono<Long> inboxCount() {
        return databaseClient()
                .sql("SELECT COUNT(*) AS count FROM reward_batch_impact_inbox")
                .map((row, metadata) -> row.get("count", Long.class))
                .one();
    }

    private static Mono<RewardBatch> batchForGrouping(String month) {
        return Mono.from(dslContext.selectFrom(
                        it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES
                )
                .where(it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES
                        .INITIATIVE_ID.eq(INITIATIVE_ID))
                .and(it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES
                        .MERCHANT_ID.eq(MERCHANT_ID))
                .and(it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES
                        .MONTH.eq(month)))
                .map(new RewardBatchSqlMapper(JsonMapper.builder().build())::fromRecord);
    }

    private static Mono<BatchAggregate> aggregateForGrouping(String month) {
        return databaseClient()
                .sql("""
                        SELECT
                            COUNT(*) AS number_of_transactions,
                            COALESCE(SUM(accrued_reward_cents), 0) AS initial_amount_cents,
                            COUNT(*) FILTER (WHERE reward_batch_trx_status = 'SUSPENDED') AS suspended_count
                        FROM reward_transactions
                        WHERE reward_batch_id = (
                            SELECT id
                            FROM reward_batches
                            WHERE initiative_id = :initiativeId
                              AND merchant_id = :merchantId
                              AND pos_type = 'PHYSICAL'
                              AND month = :month
                        )
                        """)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("merchantId", MERCHANT_ID)
                .bind("month", month)
                .map((row, metadata) -> aggregate(row))
                .one();
    }

    private static Mono<BatchAggregate> aggregate(String batchId) {
        return databaseClient()
                .sql("""
                        SELECT
                            COUNT(*) AS number_of_transactions,
                            COALESCE(SUM(accrued_reward_cents), 0) AS initial_amount_cents,
                            COUNT(*) FILTER (WHERE reward_batch_trx_status = 'SUSPENDED') AS suspended_count
                        FROM reward_transactions
                        WHERE reward_batch_id = :batchId
                        """)
                .bind("batchId", batchId)
                .map((row, metadata) -> aggregate(row))
                .one();
    }

    private static BatchAggregate aggregate(io.r2dbc.spi.Readable row) {
        return new BatchAggregate(
                row.get("number_of_transactions", Long.class),
                row.get("initial_amount_cents", Long.class),
                row.get("suspended_count", Long.class)
        );
    }

    private static Mono<TransactionState> transactionState(String transactionId) {
        return databaseClient()
                .sql("""
                        SELECT status, transaction_revision, reward_batch_id, reward_batch_trx_status, sampling_key
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", transactionId)
                .map((row, metadata) -> new TransactionState(
                        row.get("status", String.class),
                        row.get("transaction_revision", Long.class),
                        row.get("reward_batch_id", String.class),
                        row.get("reward_batch_trx_status", String.class),
                        row.get("sampling_key", Integer.class)
                ))
                .one();
    }

    private static Mono<String> transactionMerchant(String transactionId) {
        return databaseClient()
                .sql("""
                        SELECT merchant_id
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", transactionId)
                .map((row, metadata) -> row.get("merchant_id", String.class))
                .one();
    }

    private record BatchAggregate(long numberOfTransactions, long initialAmountCents, long suspendedCount) {
    }

    private record TransactionState(
            String status,
            long revision,
            String batchId,
            String batchStatus,
            int samplingKey
    ) {
    }
}
