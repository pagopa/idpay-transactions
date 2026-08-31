package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.r2dbc.spi.Connection;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
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
                .sql("DELETE FROM reward_transactions")
                .fetch()
                .rowsUpdated()
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

        StepVerifier.create(latestAppliedImpactRevision(transactionId))
                .expectNext(6L)
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
    void shouldRollBackProjectionWhenMembershipMoveFails() {
        String transactionId = "rollback-membership-failure";

        try {
            StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                            .then(insertMembership(
                                    transactionId,
                                    SOURCE_BATCH_ID,
                                    RewardBatchTrxStatus.CONSULTABLE,
                                    SyncTrxStatus.AUTHORIZED,
                                    5L,
                                    11
                            ))
                            .then(databaseClient().sql("""
                                    CREATE OR REPLACE FUNCTION fail_reward_batch_membership_move()
                                    RETURNS trigger AS $$
                                    BEGIN
                                        IF NEW.reward_batch_id IS DISTINCT FROM OLD.reward_batch_id THEN
                                            RAISE EXCEPTION 'forced membership move failure';
                                        END IF;
                                        RETURN NEW;
                                    END;
                                    $$ LANGUAGE plpgsql
                                    """).then())
                            .then(databaseClient().sql("""
                                    CREATE TRIGGER fail_reward_batch_membership_move
                                    BEFORE UPDATE ON reward_transactions
                                    FOR EACH ROW EXECUTE FUNCTION fail_reward_batch_membership_move()
                                    """).then())
                            .then(adapter.applyImpact(replacement(
                                    transactionId,
                                    "rollback-membership-failure-event",
                                    6L,
                                    eventTime()
                            ))))
                    .expectErrorMatches(error -> error.getMessage()
                            .contains("forced membership move failure"))
                    .verify();

            StepVerifier.create(transactionState(transactionId))
                    .expectNext(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            11
                    ))
                    .verifyComplete();
        } finally {
            databaseClient().sql(
                            "DROP TRIGGER IF EXISTS fail_reward_batch_membership_move ON reward_transactions")
                    .then()
                    .then(databaseClient().sql(
                            "DROP FUNCTION IF EXISTS fail_reward_batch_membership_move()").then())
                    .block();
        }
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

        StepVerifier.create(Mono.zip(latestAppliedImpactRevision(transactionId), batchCount()))
                .assertNext(result -> {
                    assertEquals(6L, result.getT1());
                    assertEquals(2L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldIgnoreALowerStaleImpactRevision() {
        String transactionId = "stale-lower-revision";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                4L,
                                10
                        ))
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "stale-lower-revision-replacement",
                                6L,
                                julyEventTime()
                        )))
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "stale-lower-revision-retry",
                                5L,
                                julyEventTime()
                        ))))
                .assertNext(transaction -> {
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED, transaction.getRewardBatchTrxStatus());
                    assertEquals(SyncTrxStatus.INVOICED.name(), transaction.getStatus());
                    assertEquals(6L, transaction.getTransactionRevision());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(transactionState(transactionId), latestAppliedImpactRevision(transactionId)))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.INVOICED.name(),
                            6L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.SUSPENDED.name(),
                            10
                    ), result.getT1());
                    assertEquals(6L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectEnvelopeAndCanonicalRevisionMismatchWithoutMutatingSource() {
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

        StepVerifier.create(Mono.zip(transactionState(transactionId), latestAppliedImpactRevision(transactionId)))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            10
                    ), result.getT1());
                    assertEquals(5L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldProcessDifferentTransactionsIndependentlyEvenWhenAnEventIdIsReused() {
        String firstTransactionId = "event-id-first";
        String secondTransactionId = "event-id-second";
        String secondSourceBatchId = "event-id-second-source";

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.CREATED, "2026-07"),
                        insertBatch(secondSourceBatchId, RewardBatchStatus.CREATED, "2026-06"),
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
                .assertNext(transaction -> {
                    assertEquals(secondTransactionId, transaction.getId());
                    assertEquals(SyncTrxStatus.INVOICED.name(), transaction.getStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        transactionState(firstTransactionId),
                        transactionState(secondTransactionId),
                        latestAppliedImpactRevision(firstTransactionId),
                        latestAppliedImpactRevision(secondTransactionId)
                ))
                .assertNext(result -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), result.getT1().status());
                    assertEquals(SyncTrxStatus.INVOICED.name(), result.getT2().status());
                    assertEquals(6L, result.getT3());
                    assertEquals(6L, result.getT4());
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
                        latestAppliedImpactRevision(firstTransactionId),
                        latestAppliedImpactRevision(secondTransactionId),
                        batchCount()
                ))
                .assertNext(result -> {
                    assertEquals(secondSourceBatchId, result.getT1().batchId());
                    assertEquals(firstSourceBatchId, result.getT2().batchId());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED.name(), result.getT1().batchStatus());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED.name(), result.getT2().batchStatus());
                    assertEquals(6L, result.getT3());
                    assertEquals(6L, result.getT4());
                    assertEquals(2L, result.getT5());
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

        StepVerifier.create(latestAppliedImpactRevision(transactionId))
                .expectNext(6L)
                .verifyComplete();
    }

    @Test
    void shouldIgnoreReplacementWhenTheSameRevisionWasAlreadyApplied() {
        String transactionId = "generic-before-replacement";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.CREATED, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                14
                        ))
                        .then(transactionAdapter.upsert(generic(
                                transactionId,
                                SyncTrxStatus.AUTHORIZED,
                                6L
                        )))
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "generic-before-replacement-event",
                                6L,
                                eventTime()
                        ))))
                .assertNext(transaction -> {
                            assertEquals(SyncTrxStatus.AUTHORIZED.name(), transaction.getStatus());
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, transaction.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        transactionState(transactionId),
                        latestAppliedImpactRevision(transactionId)
                ))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            6L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            14
                    ), result.getT1());
                    assertEquals(6L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldKeepAssignedMembershipWhenSameRevisionGenericRefundedArrivesAfterReplacement() {
        String transactionId = "replacement-before-same-revision-refunded";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.CREATED, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                15
                        ))
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "replacement-before-same-revision-refunded-event",
                                6L,
                                eventTime()
                        )))
                        .then(transactionAdapter.upsert(generic(
                                transactionId,
                                SyncTrxStatus.REFUNDED,
                                6L
                        ))))
                .assertNext(transaction -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), transaction.getStatus());
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, transaction.getRewardBatchTrxStatus());
                    assertEquals(15, transaction.getSamplingKey());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        transactionState(transactionId),
                        latestAppliedImpactRevision(transactionId)
                ))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.INVOICED.name(),
                            6L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            15
                    ), result.getT1());
                    assertEquals(6L, result.getT2());
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
        older.setInvoiceUploadDate(LocalDateTime.of(2026, Month.JULY, 1, 9, 15));
        RewardTransaction canonical = generic(transactionId, SyncTrxStatus.INVOICED, 6L);
        canonical.setFranchiseName("Updated franchise");
        canonical.setPointOfSaleType(PosType.PHYSICAL);
        canonical.setBusinessName("Updated business");
        canonical.setInvoiceUploadDate(LocalDateTime.of(2026, Month.AUGUST, 1, 9, 15));

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
                    assertEquals(
                            LocalDateTime.of(2026, Month.AUGUST, 1, 9, 15),
                            transaction.getInvoiceUploadDate()
                    );
                })
                .verifyComplete();
    }

    @Test
    void shouldIgnoreReplacementOlderThanTheCanonicalProjection() {
        String transactionId = "newer-generic-then-older-impact";
        RewardTransaction newerGeneric = generic(transactionId, SyncTrxStatus.AUTHORIZED, 10L);
        newerGeneric.setBusinessName("Newer business");

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.SENT, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                9
                        ))
                        .then(transactionAdapter.upsert(newerGeneric))
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "newer-generic-then-older-impact-event",
                                6L,
                                julyEventTime()
                        ))))
                .assertNext(transaction -> {
                    assertEquals("Newer business", transaction.getBusinessName());
                    assertEquals(SyncTrxStatus.AUTHORIZED.name(), transaction.getStatus());
                    assertEquals(10L, transaction.getTransactionRevision());
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, transaction.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(latestAppliedImpactRevision(transactionId))
                .expectNext(10L)
                .verifyComplete();

        StepVerifier.create(adapter.applyImpact(replacement(
                        transactionId,
                        "newer-generic-then-older-impact-retry",
                        6L,
                        julyEventTime()
                )))
                .assertNext(transaction -> {
                    assertEquals("Newer business", transaction.getBusinessName());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, transaction.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(latestAppliedImpactRevision(transactionId))
                .expectNext(10L)
                .verifyComplete();
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("localOnlyImpactFields")
    void shouldRejectImpactContainingLocalOnlyFieldsBeforeMutatingTheTransaction(
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

        StepVerifier.create(Mono.zip(transactionState(transactionId), latestAppliedImpactRevision(transactionId)))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            null,
                            null,
                            0
                    ), result.getT1());
                    assertEquals(5L, result.getT2());
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
                        .then(adapter.applyImpact(replacement(
                                transactionId,
                                "later-replacement",
                                7L,
                                julyEventTime()
                        )))
                        .then(transactionAdapter.upsert(generic(
                                transactionId,
                                SyncTrxStatus.AUTHORIZED,
                                6L
                        ))))
                .assertNext(transaction -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), transaction.getStatus());
                    assertEquals(7L, transaction.getTransactionRevision());
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED, transaction.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(latestAppliedImpactRevision(transactionId))
                .expectNext(7L)
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

        StepVerifier.create(Mono.zip(transactionState(transactionId), latestAppliedImpactRevision(transactionId)))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            7
                    ), result.getT1());
                    assertEquals(5L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectCanonicalMerchantMismatchWithoutMutatingSourceOrMetadata() {
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

        StepVerifier.create(Mono.zip(
                        transactionState(transactionId),
                        transactionMerchant(transactionId),
                        latestAppliedImpactRevision(transactionId)
                ))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            7
                    ), result.getT1());
                    assertEquals(MERCHANT_ID, result.getT2());
                    assertEquals(5L, result.getT3());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectInitiativeMismatchEvenForAnEqualStaleImpactRevision() {
        String transactionId = "stale-initiative-mismatch";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.CREATED, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                41
                        ))
                        .then(adapter.applyImpact(replacement(transactionId, "first-event", 6L, eventTime()))))
                .assertNext(transaction -> assertEquals(6L, transaction.getTransactionRevision()))
                .verifyComplete();

        // The impact below carries the same canonical revision (6): on its own that
        // would make it a no-op duplicate. But it also
        // conflicts on initiative, so the integrity check must still fire and reject it
        // rather than silently swallowing a malformed message as "just a duplicate".
        RewardTransaction conflicting = generic(transactionId, SyncTrxStatus.INVOICED, 6L);
        conflicting.setInitiatives(List.of("other-initiative"));

        StepVerifier.create(adapter.applyImpact(new PaymentRewardBatchImpact(
                        "stale-initiative-mismatch-event",
                        1,
                        PaymentRewardBatchImpactType.INVOICE_REPLACED,
                        eventTime(),
                        6L,
                        conflicting
                )))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains(INITIATIVE_ID))
                .verify();

        StepVerifier.create(Mono.zip(transactionState(transactionId), latestAppliedImpactRevision(transactionId)))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.INVOICED.name(),
                            6L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            41
                    ), result.getT1());
                    assertEquals(6L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectMerchantMismatchEvenForALowerStaleImpactRevision() {
        String transactionId = "stale-merchant-mismatch";

        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, RewardBatchStatus.CREATED, "2026-07")
                        .then(insertMembership(
                                transactionId,
                                SOURCE_BATCH_ID,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                41
                        ))
                        .then(adapter.applyImpact(replacement(transactionId, "first-event", 6L, eventTime()))))
                .assertNext(transaction -> assertEquals(6L, transaction.getTransactionRevision()))
                .verifyComplete();

        // Revision 5 is now lower than the canonical transaction revision (6): on its own
        // that would make it a stale duplicate to ignore. But it also conflicts on
        // merchant, so the integrity check must still fire and reject it rather than
        // silently swallowing a malformed message as "just stale".
        RewardTransaction conflicting = generic(transactionId, SyncTrxStatus.INVOICED, 5L);
        conflicting.setMerchantId("other-merchant");

        StepVerifier.create(adapter.applyImpact(new PaymentRewardBatchImpact(
                        "stale-merchant-mismatch-event",
                        1,
                        PaymentRewardBatchImpactType.INVOICE_REPLACED,
                        eventTime(),
                        5L,
                        conflicting
                )))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("source batch merchant " + MERCHANT_ID))
                .verify();

        StepVerifier.create(Mono.zip(
                        transactionState(transactionId),
                        transactionMerchant(transactionId),
                        latestAppliedImpactRevision(transactionId)
                ))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.INVOICED.name(),
                            6L,
                            SOURCE_BATCH_ID,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            41
                    ), result.getT1());
                    assertEquals(MERCHANT_ID, result.getT2());
                    assertEquals(6L, result.getT3());
                })
                .verifyComplete();
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("invalidImpacts")
    void shouldRejectMalformedImpactEnvelopesWithoutMutatingAnyState(
            String caseName,
            BiFunction<String, String, PaymentRewardBatchImpact> impactFactory,
            String expectedMessageFragment
    ) {
        String transactionId = "invalid-" + caseName;
        String eventId = "invalid-" + caseName + "-event";

        StepVerifier.create(transactionAdapter.upsert(generic(transactionId, SyncTrxStatus.AUTHORIZED, 5L))
                        .then(adapter.applyImpact(impactFactory.apply(transactionId, eventId))))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains(expectedMessageFragment))
                .verify();

        StepVerifier.create(Mono.zip(transactionState(transactionId), latestAppliedImpactRevision(transactionId)))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.AUTHORIZED.name(),
                            5L,
                            null,
                            null,
                            0
                    ), result.getT1());
                    assertEquals(5L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldResolveAMembershipAssignedConcurrentlyWithImpactCreationOfTheSameTransaction() {
        String transactionId = "conflict-created-assigned";
        RewardTransaction created = generic(transactionId, SyncTrxStatus.INVOICED, 5L);
        RewardBatch candidate = RewardBatchFactory.create(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07",
                "Business"
        );

        StepVerifier.create(Flux.merge(
                        Mono.defer(() -> assignmentAdapter.assignInvoicedTransaction(created, candidate, 17)),
                        Mono.defer(() -> adapter.applyImpact(replacement(
                                transactionId,
                                "conflict-created-assigned-event",
                                6L,
                                eventTime()
                        )))
                ).then())
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        transactionState(transactionId),
                        latestAppliedImpactRevision(transactionId),
                        databaseClient()
                                .sql("""
                                        SELECT COUNT(*) AS count
                                        FROM reward_transactions
                                        WHERE transaction_id = :transactionId
                                        """)
                                .bind("transactionId", transactionId)
                                .map((row, metadata) -> row.get("count", Long.class))
                                .one()
                ))
                .assertNext(result -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), result.getT1().status());
                    assertEquals(6L, result.getT1().revision());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE.name(), result.getT1().batchStatus());
                    assertEquals(6L, result.getT2());
                    assertEquals(1L, result.getT3());
                })
                .verifyComplete();
    }

    @Test
    void shouldReObserveAndApplyExactlyOnceWhenAConcurrentAssignmentRacesAnUnassignedTransactionObservation() {
        String transactionId = "race-observed-unassigned";
        String batchId = "race-observed-unassigned-batch";

        // A holds the transaction row locked; C's raw assignment queues behind A; only once
        // both C and the impact (B) are confirmed queued behind A do we release it, guaranteeing
        // B's initial unlocked read captured the still-unassigned row before C committed.
        StepVerifier.create(transactionAdapter.upsert(generic(transactionId, SyncTrxStatus.AUTHORIZED, 5L))
                        .then(insertBatch(batchId, RewardBatchStatus.CREATED, "2026-07")))
                .verifyComplete();

        Connection lockHolder = openConnection();
        try {
            lockTransactionRowForUpdate(lockHolder, transactionId);

            Mono<Long> concurrentAssignment = Mono.from(connectionFactory().create())
                    .flatMap(connection -> Mono.from(connection.beginTransaction())
                            .then(Mono.from(connection.createStatement("""
                                            UPDATE reward_transactions
                                            SET reward_batch_id = $1, reward_batch_trx_status = 'CONSULTABLE',
                                                sampling_key = 41
                                            WHERE transaction_id = $2
                                            """)
                                    .bind(0, batchId)
                                    .bind(1, transactionId)
                                    .execute()))
                            .flatMap(result -> Mono.from(result.getRowsUpdated()))
                            .flatMap(rows -> Mono.from(connection.commitTransaction()).thenReturn(rows))
                            .flatMap(rows -> Mono.from(connection.close()).thenReturn(rows)));
            CompletableFuture<Long> assignmentFuture = concurrentAssignment.toFuture();
            awaitBlockedBackends(1);

            CompletableFuture<RewardTransaction> impactFuture = adapter.applyImpact(replacement(
                    transactionId,
                    "race-observed-unassigned-event",
                    6L,
                    eventTime()
            )).toFuture();
            awaitBlockedBackends(2);

            releaseConnection(lockHolder);
            lockHolder = null;

            assertEquals(1L, assignmentFuture.join());
            RewardTransaction applied = impactFuture.join();
            assertEquals(SyncTrxStatus.INVOICED.name(), applied.getStatus());
            assertEquals(6L, applied.getTransactionRevision());
            assertEquals(batchId, applied.getRewardBatchId());
        } finally {
            if (lockHolder != null) {
                releaseConnection(lockHolder);
            }
        }

        StepVerifier.create(Mono.zip(
                        transactionState(transactionId),
                        latestAppliedImpactRevision(transactionId),
                        batchCount()
                ))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.INVOICED.name(),
                            6L,
                            batchId,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            41
                    ), result.getT1());
                    assertEquals(6L, result.getT2());
                    assertEquals(1L, result.getT3());
                })
                .verifyComplete();
    }

    @Test
    void shouldReObserveAndApplyExactlyOnceWhenAConcurrentReassignmentRacesTheLockedSourceMembership() {
        String transactionId = "race-observed-reassigned";
        String sourceBatchId = "race-reassign-source-batch";
        String targetBatchId = "race-reassign-target-batch";

        // A holds the transaction row locked while it still belongs to sourceBatchId; C's raw
        // reassignment to targetBatchId queues behind A; only once both C and the impact (B) are
        // confirmed queued behind A do we release it, guaranteeing B's initial unlocked read (and
        // its first, uncontended batch lock on the OLD source) captured the stale membership.
        StepVerifier.create(insertBatch(sourceBatchId, RewardBatchStatus.CREATED, "2026-07")
                        .then(insertBatch(targetBatchId, RewardBatchStatus.CREATED, "2026-08"))
                        .then(insertMembership(
                                transactionId,
                                sourceBatchId,
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.AUTHORIZED,
                                5L,
                                41
                        )))
                .verifyComplete();

        Connection lockHolder = openConnection();
        try {
            lockTransactionRowForUpdate(lockHolder, transactionId);

            Mono<Long> concurrentReassignment = Mono.from(connectionFactory().create())
                    .flatMap(connection -> Mono.from(connection.beginTransaction())
                            .then(Mono.from(connection.createStatement("""
                                            UPDATE reward_transactions
                                            SET reward_batch_id = $1, sampling_key = 55
                                            WHERE transaction_id = $2
                                            """)
                                    .bind(0, targetBatchId)
                                    .bind(1, transactionId)
                                    .execute()))
                            .flatMap(result -> Mono.from(result.getRowsUpdated()))
                            .flatMap(rows -> Mono.from(connection.commitTransaction()).thenReturn(rows))
                            .flatMap(rows -> Mono.from(connection.close()).thenReturn(rows)));
            CompletableFuture<Long> reassignmentFuture = concurrentReassignment.toFuture();
            awaitBlockedBackends(1);

            CompletableFuture<RewardTransaction> impactFuture = adapter.applyImpact(replacement(
                    transactionId,
                    "race-observed-reassigned-event",
                    6L,
                    eventTime()
            )).toFuture();
            awaitBlockedBackends(2);

            releaseConnection(lockHolder);
            lockHolder = null;

            assertEquals(1L, reassignmentFuture.join());
            RewardTransaction applied = impactFuture.join();
            assertEquals(SyncTrxStatus.INVOICED.name(), applied.getStatus());
            assertEquals(6L, applied.getTransactionRevision());
            assertEquals(targetBatchId, applied.getRewardBatchId());
        } finally {
            if (lockHolder != null) {
                releaseConnection(lockHolder);
            }
        }

        StepVerifier.create(Mono.zip(
                        transactionState(transactionId),
                        latestAppliedImpactRevision(transactionId),
                        batchCount()
                ))
                .assertNext(result -> {
                    assertEquals(new TransactionState(
                            SyncTrxStatus.INVOICED.name(),
                            6L,
                            targetBatchId,
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            55
                    ), result.getT1());
                    assertEquals(6L, result.getT2());
                    // Exactly the two pre-existing batches remain: no outcome batch was created,
                    // proving the impact retried onto the reassigned target rather than moving it
                    // (or duplicating a batch) as if it were still bound to the stale source.
                    assertEquals(2L, result.getT3());
                })
                .verifyComplete();
    }

    @Test
    void shouldReturnPaymentEligibilityByTransactionId() {
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
                        .then(adapter.findEligibility(transactionId)))
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

    }

    @Test
    void shouldExcludeUnassignedAndRejectBatchMerchantMismatchedRowsFromPaymentEligibility() {
        String unassignedTransactionId = "unassigned-eligibility";
        String mismatchedTransactionId = "mismatched-eligibility";

        StepVerifier.create(Flux.concat(
                        transactionAdapter.upsert(generic(
                                unassignedTransactionId,
                                SyncTrxStatus.INVOICED,
                                5L
                        )),
                        insertBatch(
                                "merchant-mismatched-batch",
                                RewardBatchStatus.EVALUATING,
                                "2026-09",
                                "other-batch-merchant"
                        ),
                        insertMembership(
                                mismatchedTransactionId,
                                "merchant-mismatched-batch",
                                RewardBatchTrxStatus.CONSULTABLE,
                                SyncTrxStatus.INVOICED,
                                5L,
                                5
                        )
                ).then())
                .verifyComplete();

        StepVerifier.create(adapter.findEligibility(unassignedTransactionId))
                .verifyComplete();

        StepVerifier.create(adapter.findEligibility(mismatchedTransactionId))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("merchant does not match"))
                .verify();
    }

    private static OffsetDateTime eventTime() {
        return OffsetDateTime.of(
                LocalDateTime.of(2026, Month.JULY, 31, 22, 30),
                ZoneOffset.UTC
        );
    }

    private static OffsetDateTime julyEventTime() {
        return OffsetDateTime.of(
                LocalDateTime.of(2026, Month.JULY, 1, 10, 30),
                ZoneOffset.UTC
        );
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

    private static Stream<Arguments> invalidImpacts() {
        return Stream.of(
                Arguments.of(
                        "null-impact",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> null,
                        "incomplete"
                ),
                Arguments.of(
                        "blank-event-id",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> new PaymentRewardBatchImpact(
                                " ",
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                generic(txId, SyncTrxStatus.INVOICED, 6L)
                        ),
                        "incomplete"
                ),
                Arguments.of(
                        "schema-version-below-one",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> new PaymentRewardBatchImpact(
                                eventId,
                                0,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                generic(txId, SyncTrxStatus.INVOICED, 6L)
                        ),
                        "incomplete"
                ),
                Arguments.of(
                        "null-impact-type",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> new PaymentRewardBatchImpact(
                                eventId,
                                1,
                                null,
                                eventTime(),
                                6L,
                                generic(txId, SyncTrxStatus.INVOICED, 6L)
                        ),
                        "incomplete"
                ),
                Arguments.of(
                        "null-occurred-at",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> new PaymentRewardBatchImpact(
                                eventId,
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                null,
                                6L,
                                generic(txId, SyncTrxStatus.INVOICED, 6L)
                        ),
                        "incomplete"
                ),
                Arguments.of(
                        "revision-below-one",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> new PaymentRewardBatchImpact(
                                eventId,
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                0L,
                                generic(txId, SyncTrxStatus.INVOICED, 0L)
                        ),
                        "incomplete"
                ),
                Arguments.of(
                        "null-transaction",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> new PaymentRewardBatchImpact(
                                eventId,
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                null
                        ),
                        "incomplete"
                ),
                Arguments.of(
                        "blank-transaction-id",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setId(" ");
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "incomplete"
                ),
                Arguments.of(
                        "null-merchant-id",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setMerchantId(null);
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "incomplete"
                ),
                Arguments.of(
                        "null-initiatives",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setInitiatives(null);
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "exactly one initiative"
                ),
                Arguments.of(
                        "empty-initiatives",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setInitiatives(List.of());
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "exactly one initiative"
                ),
                Arguments.of(
                        "multiple-initiatives",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setInitiatives(List.of(INITIATIVE_ID, "other-initiative"));
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "exactly one initiative"
                ),
                Arguments.of(
                        "blank-initiative",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setInitiatives(List.of(" "));
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "exactly one initiative"
                ),
                Arguments.of(
                        "invoice-replaced-wrong-status",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> new PaymentRewardBatchImpact(
                                eventId,
                                1,
                                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                eventTime(),
                                6L,
                                generic(txId, SyncTrxStatus.AUTHORIZED, 6L)
                        ),
                        "must be INVOICED"
                ),
                Arguments.of(
                        "invoice-replaced-missing-point-of-sale-type",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setPointOfSaleType(null);
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "point of sale type"
                ),
                Arguments.of(
                        "invoice-replaced-missing-business-name",
                        (BiFunction<String, String, PaymentRewardBatchImpact>) (txId, eventId) -> {
                            RewardTransaction transaction = generic(txId, SyncTrxStatus.INVOICED, 6L);
                            transaction.setBusinessName(" ");
                            return new PaymentRewardBatchImpact(
                                    eventId,
                                    1,
                                    PaymentRewardBatchImpactType.INVOICE_REPLACED,
                                    eventTime(),
                                    6L,
                                    transaction
                            );
                        },
                        "business name"
                )
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
                .trxChargeDate(LocalDateTime.of(2026, Month.JULY, 1, 10, 30))
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(125L).build()))
                .build();
    }

    private static Mono<Void> insertBatch(String batchId, RewardBatchStatus status, String month) {
        return insertBatch(batchId, status, month, MERCHANT_ID);
    }

    private static Mono<Void> insertBatch(
            String batchId,
            RewardBatchStatus status,
            String month,
            String merchantId
    ) {
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
                .bind("merchantId", merchantId)
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

    /**
     * Opens a dedicated, manually managed connection outside the shared pool used by the
     * adapter under test, so the caller can hold an explicit row lock across several reactive
     * steps without it being released or reused by unrelated operations.
     */
    private static Connection openConnection() {
        return Mono.from(connectionFactory().create()).block();
    }

    /**
     * Begins a transaction on the given connection and locks the given transaction row with
     * {@code SELECT ... FOR UPDATE}, blocking until the lock is acquired (uncontended, so this
     * returns immediately in practice) before returning control to the caller.
     */
    private static void lockTransactionRowForUpdate(Connection connection, String transactionId) {
        Mono.from(connection.beginTransaction())
                .then(Mono.from(connection.createStatement(
                                "SELECT transaction_id FROM reward_transactions WHERE transaction_id = $1 FOR UPDATE")
                        .bind(0, transactionId)
                        .execute()))
                .flatMapMany(result -> result.map((row, metadata) -> row.get("transaction_id", String.class)))
                .then()
                .block();
    }

    private static void releaseConnection(Connection connection) {
        Mono.from(connection.rollbackTransaction())
                .then(Mono.from(connection.close()))
                .block();
    }

    /**
     * Polls {@code pg_stat_activity} (a real, observable server-side fact, not a fixed sleep)
     * until at least {@code expectedCount} other backends are registered as waiting on some
     * lock, bounded by a generous timeout used only as a failure net. This lets tests
     * deterministically confirm that a concurrent writer - and, afterwards, the impact under
     * test - have actually queued behind a held row lock before releasing it, instead of
     * assuming a fixed delay is "probably enough". A row already locked via
     * {@code SELECT ... FOR UPDATE} makes a conflicting writer wait on the lock holder's
     * transaction id (a {@code transactionid} lock), not a {@code tuple}/relation lock, so
     * counting blocked backends directly (rather than filtering {@code pg_locks} by relation)
     * is what actually observes the wait deterministically here.
     */
    private static void awaitBlockedBackends(long expectedCount) {
        Mono.defer(() -> databaseClient()
                        .sql("""
                                SELECT COUNT(*) AS count
                                FROM pg_stat_activity
                                WHERE wait_event_type = 'Lock'
                                  AND pid <> pg_backend_pid()
                                """)
                        .map((row, metadata) -> row.get("count", Long.class))
                        .one())
                .filter(count -> count >= expectedCount)
                .repeatWhenEmpty(repeat -> repeat.delayElements(Duration.ofMillis(20)))
                .timeout(Duration.ofSeconds(10))
                .block();
    }

    private static Mono<Long> latestAppliedImpactRevision(String transactionId) {
        // Kept as a local helper name to minimize churn in the pre-existing impact suite.
        // Step 2 assertions now read the canonical transaction revision.
        return databaseClient()
                .sql("""
                        SELECT transaction_revision AS revision
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", transactionId)
                .map((row, metadata) -> row.get("revision", Long.class))
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
