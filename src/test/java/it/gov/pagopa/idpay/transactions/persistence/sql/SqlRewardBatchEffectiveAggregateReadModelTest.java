package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchEffectiveAggregateReadModelTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String MERCHANT_ID = "merchant-1";

    private static SqlRewardBatchListAdapter listAdapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        listAdapter = new SqlRewardBatchListAdapter(
                DSL.using(
                        new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                        SQLDialect.POSTGRES
                ),
                new RewardBatchSqlMapper(JsonMapper.builder().build())
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

    @ParameterizedTest(name = "{0} should expose effective initial={1} and suspended={2}")
    @MethodSource("effectiveAmountStates")
    void shouldProjectEffectiveInitialAndSuspendedAmountsAcrossPersistedBatchStates(
            RewardBatchStatus status,
            long expectedInitialAmountCents,
            long expectedSuspendedAmountCents,
            Long initialAmountCentsAtSend,
            Long suspendedAmountCentsAtApproving
    ) {
        String batchId = "batch-state-" + status.name().toLowerCase();

        StepVerifier.create(insertBatch(
                        batchId,
                        status,
                        "2026-07",
                        initialAmountCentsAtSend,
                        suspendedAmountCentsAtApproving
                )
                        .thenMany(Flux.concat(
                                insertTransaction(batchId, "to-check", RewardBatchTrxStatus.TO_CHECK, 100L),
                                insertTransaction(batchId, "consultable", RewardBatchTrxStatus.CONSULTABLE, 200L),
                                insertTransaction(batchId, "approved", RewardBatchTrxStatus.APPROVED, 300L),
                                insertTransaction(batchId, "suspended", RewardBatchTrxStatus.SUSPENDED, 400L),
                                insertTransaction(batchId, "rejected", RewardBatchTrxStatus.REJECTED, 500L)
                        ))
                        .then(listAdapter.findBatch(batchId)))
                .assertNext(batch -> {
                    assertEquals(expectedInitialAmountCents, batch.getInitialAmountCents());
                    assertEquals(expectedSuspendedAmountCents, batch.getSuspendedAmountCents());
                })
                .verifyComplete();
    }

    @ParameterizedTest(name = "{0} should expose approved amount {3}")
    @MethodSource("approvedAmountStates")
    void shouldGateApprovedAmountByBatchLifecycleState(
            RewardBatchStatus status,
            Long initialAmountCentsAtSend,
            Long suspendedAmountCentsAtApproving,
            long expectedApprovedAmountCents
    ) {
        String batchId = "batch-approved-" + status.name().toLowerCase();

        StepVerifier.create(insertBatch(
                        batchId,
                        status,
                        "2026-08",
                        initialAmountCentsAtSend,
                        suspendedAmountCentsAtApproving
                )
                        .thenMany(Flux.concat(
                                insertTransaction(batchId, "to-check", RewardBatchTrxStatus.TO_CHECK, 100L),
                                insertTransaction(batchId, "consultable", RewardBatchTrxStatus.CONSULTABLE, 200L),
                                insertTransaction(batchId, "approved-negative", RewardBatchTrxStatus.APPROVED, -50L),
                                insertTransaction(batchId, "suspended", RewardBatchTrxStatus.SUSPENDED, 400L),
                                insertTransaction(batchId, "rejected", RewardBatchTrxStatus.REJECTED, 500L)
                        ))
                        .then(listAdapter.findBatch(batchId)))
                .assertNext(batch -> assertEquals(expectedApprovedAmountCents, batch.getApprovedAmountCents()))
                .verifyComplete();
    }

    @Test
    void shouldProjectCurrentAndExcludedAmountsUsingSignedAssignedRows() {
        String batchId = "batch-current-excluded";

        StepVerifier.create(insertBatch(batchId, RewardBatchStatus.APPROVED, "2026-09", 500L, 10L)
                        .thenMany(Flux.concat(
                                insertTransaction(batchId, "approved", RewardBatchTrxStatus.APPROVED, 100L),
                                insertTransaction(batchId, "rejected-negative", RewardBatchTrxStatus.REJECTED, -40L),
                                insertTransaction(batchId, "suspended", RewardBatchTrxStatus.SUSPENDED, 10L)
                        ))
                        .then(listAdapter.findBatch(batchId)))
                .assertNext(batch -> {
                    assertLongProperty(batch, "currentAmountCents", 70L);
                    assertLongProperty(batch, "excludedAmountCents", -40L);
                })
                .verifyComplete();
    }

    @Test
    void shouldReturnZeroForEmptyAndZeroValuedAggregates() {
        String emptyBatchId = "batch-empty";
        String zeroBatchId = "batch-zero";

        StepVerifier.create(Flux.concat(
                        insertBatch(emptyBatchId, RewardBatchStatus.CREATED, "2026-10", null, null),
                        insertBatch(zeroBatchId, RewardBatchStatus.APPROVED, "2026-11", 0L, 0L),
                        insertTransaction(zeroBatchId, "zero-approved", RewardBatchTrxStatus.APPROVED, 0L),
                        insertTransaction(zeroBatchId, "zero-rejected", RewardBatchTrxStatus.REJECTED, 0L)
                ).then(Mono.zip(
                        listAdapter.findBatch(emptyBatchId),
                        listAdapter.findBatch(zeroBatchId)
                )))
                .assertNext(result -> {
                    assertZeroAggregates(result.getT1());
                    assertZeroAggregates(result.getT2());
                })
                .verifyComplete();
    }

    @ParameterizedTest(name = "{0} should fail closed when {3} is missing")
    @MethodSource("missingFrozenSnapshotStates")
    void shouldFailClosedWhenAFrozenSnapshotIsMissing(
            RewardBatchStatus status,
            Long initialAmountCentsAtSend,
            Long suspendedAmountCentsAtApproving,
            String missingSnapshotColumn
    ) {
        String batchId = "batch-missing-" + status.name().toLowerCase();

        StepVerifier.create(insertBatch(
                        batchId,
                        status,
                        "2026-12",
                        initialAmountCentsAtSend,
                        suspendedAmountCentsAtApproving
                )
                        .then(insertTransaction(batchId, "approved", RewardBatchTrxStatus.APPROVED, 100L))
                        .then(listAdapter.findBatch(batchId)))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("Data integrity")
                        && error.getMessage().contains(missingSnapshotColumn)
                        && error.getMessage().contains(batchId))
                .verify();
    }

    @Test
    void shouldKeepListAndDetailProjectionConsistent() {
        String batchId = "batch-consistency";

        StepVerifier.create(insertBatch(batchId, RewardBatchStatus.APPROVING, "2027-01", 900L, 250L)
                        .thenMany(Flux.concat(
                                insertTransaction(batchId, "to-check", RewardBatchTrxStatus.TO_CHECK, 100L),
                                insertTransaction(batchId, "approved", RewardBatchTrxStatus.APPROVED, 300L),
                                insertTransaction(batchId, "rejected", RewardBatchTrxStatus.REJECTED, 50L),
                                insertTransaction(batchId, "suspended", RewardBatchTrxStatus.SUSPENDED, 25L)
                        ))
                        .then(Mono.zip(
                                listAdapter.findBatch(batchId),
                                listAdapter.findRewardBatches(
                                                MERCHANT_ID,
                                                INITIATIVE_ID,
                                                null,
                                                null,
                                                "2027-01",
                                                false,
                                                PageRequest.of(0, 10)
                                        )
                                        .single()
                        )))
                .assertNext(result -> {
                    RewardBatch detail = result.getT1();
                    RewardBatch list = result.getT2();
                    assertEquals(detail.getInitialAmountCents(), list.getInitialAmountCents());
                    assertEquals(detail.getSuspendedAmountCents(), list.getSuspendedAmountCents());
                    assertEquals(detail.getApprovedAmountCents(), list.getApprovedAmountCents());
                    assertLongProperty(list, "currentAmountCents", readLongProperty(detail, "currentAmountCents"));
                    assertLongProperty(list, "excludedAmountCents", readLongProperty(detail, "excludedAmountCents"));
                })
                .verifyComplete();
    }

    @Test
    void shouldSortByInitialAmountUsingEffectiveDefinitions() {
        StepVerifier.create(Flux.concat(
                        insertBatch("batch-sent-frozen-low", RewardBatchStatus.SENT, "2027-02", 100L, null),
                        insertTransaction(
                                "batch-sent-frozen-low",
                                "transaction-sent",
                                RewardBatchTrxStatus.APPROVED,
                                900L
                        ),
                        insertBatch("batch-created-live-mid", RewardBatchStatus.CREATED, "2027-03", null, null),
                        insertTransaction(
                                "batch-created-live-mid",
                                "transaction-created",
                                RewardBatchTrxStatus.APPROVED,
                                200L
                        ),
                        insertBatch("batch-approved-frozen-high", RewardBatchStatus.APPROVED, "2027-04", 300L, 0L),
                        insertTransaction(
                                "batch-approved-frozen-high",
                                "transaction-approved",
                                RewardBatchTrxStatus.APPROVED,
                                50L
                        )
                ).thenMany(listAdapter.findRewardBatches(
                                MERCHANT_ID,
                                INITIATIVE_ID,
                                null,
                                null,
                                null,
                                false,
                                PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "initialAmountCents"))
                        ))
                        .map(RewardBatch::getId)
                        .collectList())
                .expectNext(List.of(
                        "batch-sent-frozen-low",
                        "batch-created-live-mid",
                        "batch-approved-frozen-high"
                ))
                .verifyComplete();
    }

    @Test
    void shouldSortBySuspendedAmountUsingEffectiveDefinitions() {
        StepVerifier.create(Flux.concat(
                        insertBatch("batch-approving-snapshot-low", RewardBatchStatus.APPROVING, "2027-05", 1000L, 10L),
                        insertTransaction(
                                "batch-approving-snapshot-low",
                                "transaction-approving",
                                RewardBatchTrxStatus.SUSPENDED,
                                1000L
                        ),
                        insertBatch("batch-evaluating-live-high", RewardBatchStatus.EVALUATING, "2027-06", 1000L, null),
                        insertTransaction(
                                "batch-evaluating-live-high",
                                "transaction-evaluating",
                                RewardBatchTrxStatus.SUSPENDED,
                                20L
                        )
                ).thenMany(listAdapter.findRewardBatches(
                                MERCHANT_ID,
                                INITIATIVE_ID,
                                null,
                                null,
                                null,
                                false,
                                PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "suspendedAmountCents"))
                        ))
                        .map(RewardBatch::getId)
                        .collectList())
                .expectNext(List.of(
                        "batch-approving-snapshot-low",
                        "batch-evaluating-live-high"
                ))
                .verifyComplete();
    }

    @Test
    void shouldSortByApprovedAmountUsingLifecycleGateDefinitions() {
        StepVerifier.create(Flux.concat(
                        insertBatch("batch-created-approved-zero", RewardBatchStatus.CREATED, "2027-07", null, null),
                        insertTransaction(
                                "batch-created-approved-zero",
                                "transaction-created-approved",
                                RewardBatchTrxStatus.TO_CHECK,
                                500L
                        ),
                        insertBatch("batch-evaluating-approved-positive", RewardBatchStatus.EVALUATING, "2027-08", 100L, null),
                        insertTransaction(
                                "batch-evaluating-approved-positive",
                                "transaction-evaluating-approved",
                                RewardBatchTrxStatus.APPROVED,
                                100L
                        )
                ).thenMany(listAdapter.findRewardBatches(
                                MERCHANT_ID,
                                INITIATIVE_ID,
                                null,
                                null,
                                null,
                                false,
                                PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "approvedAmountCents"))
                        ))
                        .map(RewardBatch::getId)
                        .collectList())
                .expectNext(List.of(
                        "batch-evaluating-approved-positive",
                        "batch-created-approved-zero"
                ))
                .verifyComplete();
    }

    private static Stream<Arguments> effectiveAmountStates() {
        return Stream.of(
                Arguments.of(RewardBatchStatus.CREATED, 1_500L, 400L, null, null),
                Arguments.of(RewardBatchStatus.SENT, 900L, 400L, 900L, null),
                Arguments.of(RewardBatchStatus.EVALUATING, 900L, 400L, 900L, null),
                Arguments.of(RewardBatchStatus.APPROVING, 900L, 250L, 900L, 250L),
                Arguments.of(RewardBatchStatus.APPROVED, 900L, 250L, 900L, 250L),
                Arguments.of(RewardBatchStatus.PENDING_REFUND, 900L, 250L, 900L, 250L),
                Arguments.of(RewardBatchStatus.NOT_REFUNDED, 900L, 250L, 900L, 250L),
                Arguments.of(RewardBatchStatus.REFUNDED, 900L, 250L, 900L, 250L)
        );
    }

    private static Stream<Arguments> approvedAmountStates() {
        return Stream.of(
                Arguments.of(RewardBatchStatus.CREATED, null, null, 0L),
                Arguments.of(RewardBatchStatus.SENT, 900L, null, 0L),
                Arguments.of(RewardBatchStatus.EVALUATING, 900L, null, 250L),
                Arguments.of(RewardBatchStatus.APPROVING, 900L, 250L, 250L),
                Arguments.of(RewardBatchStatus.APPROVED, 900L, 250L, 250L),
                Arguments.of(RewardBatchStatus.PENDING_REFUND, 900L, 250L, 250L),
                Arguments.of(RewardBatchStatus.NOT_REFUNDED, 900L, 250L, 250L),
                Arguments.of(RewardBatchStatus.REFUNDED, 900L, 250L, 250L)
        );
    }

    private static Stream<Arguments> missingFrozenSnapshotStates() {
        return Stream.of(
                Arguments.of(RewardBatchStatus.SENT, null, null, "initial_amount_cents_at_send"),
                Arguments.of(RewardBatchStatus.EVALUATING, null, null, "initial_amount_cents_at_send"),
                Arguments.of(RewardBatchStatus.APPROVING, 900L, null, "suspended_amount_cents_at_approving"),
                Arguments.of(RewardBatchStatus.APPROVED, 900L, null, "suspended_amount_cents_at_approving"),
                Arguments.of(RewardBatchStatus.PENDING_REFUND, 900L, null, "suspended_amount_cents_at_approving"),
                Arguments.of(RewardBatchStatus.NOT_REFUNDED, 900L, null, "suspended_amount_cents_at_approving"),
                Arguments.of(RewardBatchStatus.REFUNDED, 900L, null, "suspended_amount_cents_at_approving")
        );
    }

    private static Mono<Long> insertBatch(
            String batchId,
            RewardBatchStatus status,
            String month,
            Long initialAmountCentsAtSend,
            Long suspendedAmountCentsAtApproving
    ) {
        var spec = databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id,
                            initiative_id,
                            merchant_id,
                            month,
                            pos_type,
                            status,
                            name,
                            assignee_level,
                            initial_amount_cents_at_send,
                            suspended_amount_cents_at_approving
                        )
                        VALUES (
                            :id,
                            :initiativeId,
                            :merchantId,
                            :month,
                            'PHYSICAL',
                            :status,
                            :name,
                            'L1',
                            :initialAmountCentsAtSend,
                            :suspendedAmountCentsAtApproving
                        )
                        """)
                .bind("id", batchId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("merchantId", MERCHANT_ID)
                .bind("month", month)
                .bind("status", status.name())
                .bind("name", "Batch " + batchId);
        spec = initialAmountCentsAtSend == null
                ? spec.bindNull("initialAmountCentsAtSend", Long.class)
                : spec.bind("initialAmountCentsAtSend", initialAmountCentsAtSend);
        spec = suspendedAmountCentsAtApproving == null
                ? spec.bindNull("suspendedAmountCentsAtApproving", Long.class)
                : spec.bind("suspendedAmountCentsAtApproving", suspendedAmountCentsAtApproving);
        return spec.fetch().rowsUpdated();
    }

    private static Mono<Long> insertTransaction(
            String batchId,
            String transactionId,
            RewardBatchTrxStatus status,
            long amountCents
    ) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id,
                            initiative_id,
                            reward_batch_id,
                            reward_batch_trx_status,
                            accrued_reward_cents
                        )
                        VALUES (
                            :transactionId,
                            :initiativeId,
                            :rewardBatchId,
                            :rewardBatchTrxStatus,
                            :accruedRewardCents
                        )
                        """)
                .bind("transactionId", transactionId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("rewardBatchId", batchId)
                .bind("rewardBatchTrxStatus", status.name())
                .bind("accruedRewardCents", amountCents)
                .fetch()
                .rowsUpdated();
    }

    private static void assertZeroAggregates(RewardBatch batch) {
        assertEquals(0L, batch.getInitialAmountCents());
        assertEquals(0L, batch.getSuspendedAmountCents());
        assertEquals(0L, batch.getApprovedAmountCents());
        assertLongProperty(batch, "currentAmountCents", 0L);
        assertLongProperty(batch, "excludedAmountCents", 0L);
    }

    private static void assertLongProperty(Object target, String property, long expectedValue) {
        assertEquals(expectedValue, readLongProperty(target, property));
    }

    private static Long readLongProperty(Object target, String property) {
        String getterName = "get" + Character.toUpperCase(property.charAt(0)) + property.substring(1);
        try {
            Method getter = target.getClass().getMethod(getterName);
            assertEquals(Long.class, getter.getReturnType(), getterName + " should return Long");
            return (Long) getter.invoke(target);
        } catch (ReflectiveOperationException exception) {
            fail(target.getClass().getSimpleName() + " should expose Long property " + property, exception);
            return null;
        }
    }
}
