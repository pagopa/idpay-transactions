package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.r2dbc.spi.Connection;
import it.gov.pagopa.idpay.transactions.enums.PaymentRewardBatchImpactType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.PaymentRewardBatchImpact;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlSuspendedTransactionReassignmentAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String OTHER_INITIATIVE_ID = "initiative-2";
    private static final String MERCHANT_ID = "merchant-1";
    private static final String SOURCE_BATCH_ID = "source-batch";
    private static final String TARGET_BATCH_ID = "target-batch";
    private static final String FUTURE_SOURCE_BATCH_ID = "future-source-batch";

    private static final YearMonth CURRENT_MONTH = YearMonth.now(ZONEID);
    private static final String PAST_MONTH = CURRENT_MONTH.minusMonths(1).toString();
    private static final String FUTURE_MONTH = CURRENT_MONTH.plusMonths(1).toString();

    private static SqlSuspendedTransactionReassignmentAdapter adapter;
    private static SqlPaymentRewardBatchImpactAdapter paymentImpactAdapter;
    private static SqlRewardBatchListAdapter listAdapter;
    private static DSLContext dslContext;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        dslContext = DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                SQLDialect.POSTGRES
        );
        RewardBatchSqlMapper batchMapper = new RewardBatchSqlMapper(JsonMapper.builder().build());
        RewardTransactionSqlMapper transactionMapper = new RewardTransactionSqlMapper(JsonMapper.builder().build());
        SqlRewardBatchAdapter batchAdapter = new SqlRewardBatchAdapter(
                transactionalOperator(),
                dslContext,
                new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                        .getRepository(RewardBatchSqlRepository.class),
                batchMapper
        );
        adapter = new SqlSuspendedTransactionReassignmentAdapter(
                transactionalOperator(),
                connectionFactory(),
                batchAdapter,
                batchMapper
        );
        SqlRewardTransactionAdapter transactionAdapter = new SqlRewardTransactionAdapter(
                transactionalOperator(),
                dslContext,
                transactionMapper
        );
        paymentImpactAdapter = new SqlPaymentRewardBatchImpactAdapter(
                transactionalOperator(),
                connectionFactory(),
                dslContext,
                transactionAdapter,
                batchAdapter,
                transactionMapper,
                batchMapper
        );
        listAdapter = new SqlRewardBatchListAdapter(dslContext, batchMapper);
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
    void shouldMoveOnlySuspendedRowsToNewCurrentMonthBatchAndProjectBothBatchAggregates() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, PAST_MONTH, RewardBatchStatus.EVALUATING),
                        insertTransaction(
                                "suspended-without-last-month",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                100L,
                                null
                        ),
                        insertTransaction(
                                "suspended-with-last-month",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                250L,
                                "2025-01"
                        ),
                        insertTransaction(
                                "approved",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.APPROVED,
                                50L,
                                "2025-02"
                        ),
                        insertTransaction(
                                "rejected",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.REJECTED,
                                75L,
                                "2025-03"
                        ),
                        insertTransaction(
                                "consultable",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.CONSULTABLE,
                                25L,
                                "2025-04"
                        )
                ).then(adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID)))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        projectedBatch(SOURCE_BATCH_ID, RewardBatchStatus.EVALUATING),
                        projectedBatchByGrouping(CURRENT_MONTH.toString()),
                        transactionRows()
                ))
                .assertNext(result -> {
                    assertAggregate(result.getT1(), 3L, 150L, 2L, 0L, 1L, 0L, 75L);

                    RewardBatch target = result.getT2();
                    assertEquals(RewardBatchStatus.CREATED, target.getStatus());
                    assertEquals(CURRENT_MONTH.toString(), target.getMonth());
                    assertAggregate(target, 2L, 350L, 2L, 2L, 0L, 350L, 0L);

                    Map<String, TransactionRow> transactions = result.getT3();
                    assertRow(transactions.get("suspended-without-last-month"),
                            target.getId(), SyncTrxStatus.INVOICED, RewardBatchTrxStatus.SUSPENDED, PAST_MONTH);
                    assertRow(transactions.get("suspended-with-last-month"),
                            target.getId(), SyncTrxStatus.INVOICED, RewardBatchTrxStatus.SUSPENDED, "2025-01");
                    assertRow(transactions.get("approved"),
                            SOURCE_BATCH_ID, SyncTrxStatus.REWARDED, RewardBatchTrxStatus.APPROVED, "2025-02");
                    assertRow(transactions.get("rejected"),
                            SOURCE_BATCH_ID, SyncTrxStatus.REWARDED, RewardBatchTrxStatus.REJECTED, "2025-03");
                    assertRow(transactions.get("consultable"),
                            SOURCE_BATCH_ID, SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, "2025-04");
                })
                .verifyComplete();
    }

    @Test
    void shouldReuseExistingTargetAndKeepFutureSourceAsItsOwnSingleTarget() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, PAST_MONTH, RewardBatchStatus.EVALUATING),
                        insertBatch(TARGET_BATCH_ID, INITIATIVE_ID, CURRENT_MONTH.toString(),
                                RewardBatchStatus.CREATED),
                        insertBatch(FUTURE_SOURCE_BATCH_ID, INITIATIVE_ID, FUTURE_MONTH,
                                RewardBatchStatus.EVALUATING),
                        insertTransaction(
                                "past-suspended",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                100L,
                                null
                        ),
                        insertTransaction(
                                "future-suspended",
                                INITIATIVE_ID,
                                FUTURE_SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                200L,
                                null
                        )
                ).then(adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                        .then(adapter.reassignSuspendedTransactions(FUTURE_SOURCE_BATCH_ID, INITIATIVE_ID)))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        batchesInGrouping(CURRENT_MONTH.toString()),
                        batchesInGrouping(FUTURE_MONTH),
                        projectedBatch(TARGET_BATCH_ID, RewardBatchStatus.CREATED),
                        projectedBatch(FUTURE_SOURCE_BATCH_ID, RewardBatchStatus.EVALUATING),
                        transactionRows()
                ))
                .assertNext(result -> {
                    assertEquals(1L, result.getT1());
                    assertEquals(1L, result.getT2());
                    assertAggregate(result.getT3(), 1L, 100L, 1L, 1L, 0L, 100L, 0L);
                    assertAggregate(result.getT4(), 1L, 200L, 1L, 1L, 0L, 200L, 0L);

                    Map<String, TransactionRow> transactions = result.getT5();
                    assertRow(transactions.get("past-suspended"),
                            TARGET_BATCH_ID, SyncTrxStatus.INVOICED, RewardBatchTrxStatus.SUSPENDED, PAST_MONTH);
                    assertRow(transactions.get("future-suspended"),
                            FUTURE_SOURCE_BATCH_ID, SyncTrxStatus.INVOICED,
                            RewardBatchTrxStatus.SUSPENDED, FUTURE_MONTH);
                })
                .verifyComplete();
    }

    @Test
    void shouldBeIdempotentWhenReassignmentIsRetriedAfterCompletion() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, PAST_MONTH, RewardBatchStatus.EVALUATING),
                        insertTransaction(
                                "first-suspended",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                100L,
                                null
                        ),
                        insertTransaction(
                                "second-suspended",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                250L,
                                null
                        )
                ).then(adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                        .then(reassignmentSnapshot())
                        .flatMap(first -> adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID)
                                .then(reassignmentSnapshot())
                                .map(second -> Map.entry(first, second))))
                .assertNext(snapshots -> {
                    assertEquals(snapshots.getKey(), snapshots.getValue());
                    assertEquals(1L, snapshots.getKey().targetBatchCount());
                    assertEquals(0L, snapshots.getKey().source().numberOfTransactions());
                    assertEquals(2L, snapshots.getKey().target().numberOfTransactions());
                    assertEquals(350L, snapshots.getKey().target().initialAmountCents());
                    assertEquals(350L, snapshots.getKey().target().suspendedAmountCents());
                })
                .verifyComplete();
    }

    @Test
    void shouldMoveMembershipExactlyOnceWhenCommandsRunConcurrently() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, PAST_MONTH, RewardBatchStatus.EVALUATING),
                        insertTransaction(
                                "concurrent-first",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                10L,
                                null
                        ),
                        insertTransaction(
                                "concurrent-second",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                20L,
                                null
                        ),
                        insertTransaction(
                                "concurrent-third",
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                30L,
                                null
                        )
                ).then(Mono.when(
                        Mono.defer(() -> adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID)),
                        Mono.defer(() -> adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID))
                )))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        batchesInGrouping(CURRENT_MONTH.toString()),
                        projectedBatch(SOURCE_BATCH_ID, RewardBatchStatus.EVALUATING),
                        projectedBatchByGrouping(CURRENT_MONTH.toString()),
                        transactionRows()
                ))
                .assertNext(result -> {
                    assertEquals(1L, result.getT1());
                    assertAggregate(result.getT2(), 0L, 0L, 0L, 0L, 0L, 0L, 0L);
                    assertAggregate(result.getT3(), 3L, 60L, 3L, 3L, 0L, 60L, 0L);
                    result.getT4().values().forEach(row -> assertRow(
                            row,
                            result.getT3().getId(),
                            SyncTrxStatus.INVOICED,
                            RewardBatchTrxStatus.SUSPENDED,
                            PAST_MONTH
                    ));
                })
                .verifyComplete();
    }

    @Test
    void shouldCompleteReassignmentAndInvoiceReplacementAcrossOppositeBatchLockOrder() {
        String sourceBatchId = "z-past-source";
        String targetBatchId = "a-current-target";
        String reassignedTransactionId = "reassignment-suspended";
        String impactTransactionId = "impact-suspended";

        StepVerifier.create(Flux.concat(
                        insertBatch(sourceBatchId, INITIATIVE_ID, PAST_MONTH, RewardBatchStatus.EVALUATING),
                        insertBatch(targetBatchId, INITIATIVE_ID, CURRENT_MONTH.toString(),
                                RewardBatchStatus.CREATED),
                        insertTransaction(
                                reassignedTransactionId,
                                INITIATIVE_ID,
                                sourceBatchId,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                100L,
                                null
                        ),
                        insertImpactMembership(impactTransactionId, sourceBatchId)
                ).then())
                .verifyComplete();

        Connection lockHolder = openConnection();
        try {
            lockBatchForUpdate(lockHolder, sourceBatchId);

            CompletableFuture<RewardTransaction> impactFuture = paymentImpactAdapter.applyImpact(
                    invoiceReplacement(impactTransactionId)
            ).toFuture();
            awaitBlockedBackends(1);

            CompletableFuture<Void> reassignmentFuture = adapter.reassignSuspendedTransactions(
                    sourceBatchId,
                    INITIATIVE_ID
            ).toFuture();
            awaitBlockedBackends(2);

            releaseConnection(lockHolder);
            lockHolder = null;

            assertEquals(impactTransactionId, impactFuture.join().getId());
            reassignmentFuture.join();
        } finally {
            if (lockHolder != null) {
                releaseConnection(lockHolder);
            }
        }

        StepVerifier.create(Mono.zip(
                        batchesInGrouping(CURRENT_MONTH.toString()),
                        projectedBatch(sourceBatchId, RewardBatchStatus.EVALUATING),
                        projectedBatch(targetBatchId, RewardBatchStatus.CREATED),
                        transactionRows()
                ))
                .assertNext(result -> {
                    assertEquals(1L, result.getT1());
                    assertAggregate(result.getT2(), 0L, 0L, 0L, 0L, 0L, 0L, 0L);
                    assertAggregate(result.getT3(), 2L, 225L, 2L, 2L, 0L, 225L, 0L);

                    assertMovedToTarget(result.getT4().get(reassignedTransactionId), targetBatchId);
                    assertMovedToTarget(result.getT4().get(impactTransactionId), targetBatchId);
                })
                .verifyComplete();
    }

    @Test
    void shouldFailForUnknownOrOutOfScopeSourceWithoutChangingRows() {
        assertMeaningfulError(adapter.reassignSuspendedTransactions("", INITIATIVE_ID));
        assertMeaningfulError(adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, ""));
        assertMeaningfulError(adapter.reassignSuspendedTransactions("missing-source", INITIATIVE_ID));

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, OTHER_INITIATIVE_ID, PAST_MONTH,
                                RewardBatchStatus.EVALUATING),
                        insertTransaction(
                                "other-initiative-suspended",
                                OTHER_INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.SUSPENDED,
                                100L,
                                null
                        )
                ).then())
                .verifyComplete();

        assertMeaningfulError(adapter.reassignSuspendedTransactions(SOURCE_BATCH_ID, INITIATIVE_ID));

        StepVerifier.create(Mono.zip(
                        Mono.from(dslContext.selectCount()
                                        .from(REWARD_BATCHES)
                                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(INITIATIVE_ID)))
                                .map(result -> result.value1().longValue()),
                        transactionRows()
                ))
                .assertNext(result -> {
                    assertEquals(0L, result.getT1());
                    assertRow(result.getT2().get("other-initiative-suspended"),
                            SOURCE_BATCH_ID, SyncTrxStatus.REWARDED, RewardBatchTrxStatus.SUSPENDED, null);
                })
                .verifyComplete();
    }

    private static void assertMeaningfulError(Mono<Void> operation) {
        StepVerifier.create(operation)
                .expectErrorMatches(error -> error.getMessage() != null && !error.getMessage().isBlank())
                .verify();
    }

    private static Mono<Void> insertBatch(
            String batchId,
            String initiativeId,
            String month,
            RewardBatchStatus status
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
                .bind("initiativeId", initiativeId)
                .bind("merchantId", MERCHANT_ID)
                .bind("month", month)
                .bind("status", status.name())
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> insertTransaction(
            String transactionId,
            String initiativeId,
            String batchId,
            SyncTrxStatus syncStatus,
            RewardBatchTrxStatus batchStatus,
            long accruedRewardCents,
            String lastElaboratedMonth
    ) {
        var specification = databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id, status, reward_batch_trx_status,
                            accrued_reward_cents, reward_batch_last_month_elaborated
                        )
                        VALUES (
                            :id, :initiativeId, :batchId, :syncStatus, :batchStatus, :accruedRewardCents,
                            :lastElaboratedMonth
                        )
                        """)
                .bind("id", transactionId)
                .bind("initiativeId", initiativeId)
                .bind("batchId", batchId)
                .bind("syncStatus", syncStatus.name())
                .bind("batchStatus", batchStatus.name())
                .bind("accruedRewardCents", accruedRewardCents);
        if (lastElaboratedMonth == null) {
            specification = specification.bindNull("lastElaboratedMonth", String.class);
        } else {
            specification = specification.bind("lastElaboratedMonth", lastElaboratedMonth);
        }
        return specification.fetch().rowsUpdated().then();
    }

    private static Mono<Void> insertImpactMembership(String transactionId, String batchId) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, merchant_id, point_of_sale_id, pos_type,
                            point_of_sale_type, business_name, reward_batch_id, reward_batch_trx_status,
                            status, accrued_reward_cents, transaction_revision
                        )
                        VALUES (
                            :transactionId, :initiativeId, :merchantId, 'pos-1', 'PHYSICAL',
                            'PHYSICAL', 'Business', :batchId, 'SUSPENDED',
                            'REWARDED', 100, 5
                        )
                        """)
                .bind("transactionId", transactionId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("merchantId", MERCHANT_ID)
                .bind("batchId", batchId)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static PaymentRewardBatchImpact invoiceReplacement(String transactionId) {
        RewardTransaction transaction = RewardTransaction.builder()
                .id(transactionId)
                .transactionRevision(6L)
                .initiatives(List.of(INITIATIVE_ID))
                .merchantId(MERCHANT_ID)
                .pointOfSaleId("pos-1")
                .pointOfSaleType(PosType.PHYSICAL)
                .posType(PosType.PHYSICAL.name())
                .businessName("Business")
                .status(SyncTrxStatus.INVOICED.name())
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(125L).build()))
                .build();
        return new PaymentRewardBatchImpact(
                "deadlock-impact-event",
                1,
                PaymentRewardBatchImpactType.INVOICE_REPLACED,
                OffsetDateTime.now(ZONEID),
                6L,
                transaction
        );
    }

    private static Connection openConnection() {
        return Mono.from(connectionFactory().create()).block();
    }

    private static void lockBatchForUpdate(Connection connection, String batchId) {
        Mono.from(connection.beginTransaction())
                .then(Mono.from(connection.createStatement(
                                "SELECT id FROM reward_batches WHERE id = $1 FOR UPDATE")
                        .bind(0, batchId)
                        .execute()))
                .flatMapMany(result -> result.map((row, metadata) -> row.get("id", String.class)))
                .then()
                .block();
    }

    private static void releaseConnection(Connection connection) {
        Mono.from(connection.rollbackTransaction())
                .then(Mono.from(connection.close()))
                .block();
    }

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
                .repeatWhenEmpty(repeat -> repeat)
                .timeout(Duration.ofSeconds(10))
                .block();
    }

    private static Mono<RewardBatch> projectedBatch(String batchId, RewardBatchStatus status) {
        return listAdapter.findBatchesWithStatus(status, INITIATIVE_ID)
                .filter(batch -> batch.getId().equals(batchId))
                .single();
    }

    private static Mono<RewardBatch> projectedBatchByGrouping(String month) {
        return listAdapter.findBatchesWithStatus(RewardBatchStatus.CREATED, INITIATIVE_ID)
                .filter(batch -> batch.getMonth().equals(month))
                .single();
    }

    private static Mono<Long> batchesInGrouping(String month) {
        return Mono.from(dslContext.selectCount()
                        .from(REWARD_BATCHES)
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(INITIATIVE_ID))
                        .and(REWARD_BATCHES.MERCHANT_ID.eq(MERCHANT_ID))
                        .and(REWARD_BATCHES.POS_TYPE.eq("PHYSICAL"))
                        .and(REWARD_BATCHES.MONTH.eq(month)))
                .map(result -> result.value1().longValue());
    }

    private static Mono<Map<String, TransactionRow>> transactionRows() {
        return Flux.from(dslContext.select(
                                REWARD_TRANSACTIONS.TRANSACTION_ID,
                                REWARD_TRANSACTIONS.REWARD_BATCH_ID,
                                REWARD_TRANSACTIONS.STATUS,
                                REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                                REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED
                        )
                        .from(REWARD_TRANSACTIONS))
                .collectMap(
                        row -> row.get(REWARD_TRANSACTIONS.TRANSACTION_ID),
                        SqlSuspendedTransactionReassignmentAdapterTest::toTransactionRow
                );
    }

    private static Mono<ReassignmentSnapshot> reassignmentSnapshot() {
        return Mono.zip(
                        projectedBatch(SOURCE_BATCH_ID, RewardBatchStatus.EVALUATING),
                        projectedBatchByGrouping(CURRENT_MONTH.toString()),
                        batchesInGrouping(CURRENT_MONTH.toString()),
                        transactionRows()
                )
                .map(result -> new ReassignmentSnapshot(
                        batchProjection(result.getT1()),
                        batchProjection(result.getT2()),
                        result.getT3(),
                        result.getT4()
                ));
    }

    private static TransactionRow toTransactionRow(Record row) {
        return new TransactionRow(
                row.get(REWARD_TRANSACTIONS.REWARD_BATCH_ID),
                row.get(REWARD_TRANSACTIONS.STATUS),
                row.get(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS),
                row.get(REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED)
        );
    }

    private static BatchProjection batchProjection(RewardBatch batch) {
        return new BatchProjection(
                batch.getNumberOfTransactions(),
                batch.getInitialAmountCents(),
                batch.getNumberOfTransactionsElaborated(),
                batch.getNumberOfTransactionsSuspended(),
                batch.getNumberOfTransactionsRejected(),
                batch.getSuspendedAmountCents(),
                batch.getApprovedAmountCents()
        );
    }

    private static void assertAggregate(
            RewardBatch batch,
            long transactions,
            long initialAmount,
            long elaborated,
            long suspended,
            long rejected,
            long suspendedAmount,
            long approvedAmount
    ) {
        assertEquals(transactions, batch.getNumberOfTransactions());
        assertEquals(initialAmount, batch.getInitialAmountCents());
        assertEquals(elaborated, batch.getNumberOfTransactionsElaborated());
        assertEquals(suspended, batch.getNumberOfTransactionsSuspended());
        assertEquals(rejected, batch.getNumberOfTransactionsRejected());
        assertEquals(suspendedAmount, batch.getSuspendedAmountCents());
        assertEquals(approvedAmount, batch.getApprovedAmountCents());
    }

    private static void assertRow(
            TransactionRow actual,
            String batchId,
            SyncTrxStatus syncStatus,
            RewardBatchTrxStatus batchStatus,
            String lastElaboratedMonth
    ) {
        assertEquals(new TransactionRow(
                batchId,
                syncStatus.name(),
                batchStatus.name(),
                lastElaboratedMonth
        ), actual);
    }

    private static void assertMovedToTarget(TransactionRow actual, String targetBatchId) {
        assertEquals(targetBatchId, actual.batchId());
        assertEquals(SyncTrxStatus.INVOICED.name(), actual.syncStatus());
        assertEquals(RewardBatchTrxStatus.SUSPENDED.name(), actual.batchStatus());
    }

    private record TransactionRow(
            String batchId,
            String syncStatus,
            String batchStatus,
            String lastElaboratedMonth
    ) {
    }

    private record BatchProjection(
            Long numberOfTransactions,
            Long initialAmountCents,
            Long numberOfTransactionsElaborated,
            Long numberOfTransactionsSuspended,
            Long numberOfTransactionsRejected,
            Long suspendedAmountCents,
            Long approvedAmountCents
    ) {
    }

    private record ReassignmentSnapshot(
            BatchProjection source,
            BatchProjection target,
            long targetBatchCount,
            Map<String, TransactionRow> transactions
    ) {
    }
}
