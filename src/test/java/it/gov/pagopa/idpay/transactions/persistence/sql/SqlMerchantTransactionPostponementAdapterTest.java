package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.gov.pagopa.common.web.exception.ClientException;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.stream.Stream;
import org.jooq.DSLContext;
import org.jooq.Record;
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
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlMerchantTransactionPostponementAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String OTHER_INITIATIVE_ID = "initiative-2";
    private static final String MERCHANT_ID = "merchant-1";
    private static final String OTHER_MERCHANT_ID = "merchant-2";
    private static final String SOURCE_BATCH_ID = "source-batch";
    private static final String TARGET_BATCH_ID = "target-batch";
    private static final String SOURCE_MONTH = "2026-04";
    private static final String TARGET_MONTH = "2026-05";

    private static SqlMerchantTransactionPostponementAdapter adapter;
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
        adapter = new SqlMerchantTransactionPostponementAdapter(
                transactionalOperator(),
                connectionFactory(),
                batchAdapter,
                batchMapper,
                transactionMapper
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
    void shouldMoveToExistingCreatedTargetAndProjectBothBatchesFromMembership() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, SOURCE_MONTH,
                                RewardBatchStatus.CREATED),
                        insertBatch(TARGET_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, TARGET_MONTH,
                                RewardBatchStatus.CREATED),
                        insertTransaction("moved", INITIATIVE_ID, MERCHANT_ID, SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.SUSPENDED, 100L),
                        insertTransaction("source-remaining", INITIATIVE_ID, MERCHANT_ID, SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 50L),
                        insertTransaction("target-existing", INITIATIVE_ID, MERCHANT_ID, TARGET_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.TO_CHECK, 30L)
                ).then(adapter.postponeTransaction(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID,
                        "moved",
                        LocalDate.of(2026, Month.DECEMBER, 31)
                )))
                .assertNext(moved -> {
                    assertEquals("moved", moved.getId());
                    assertEquals(TARGET_BATCH_ID, moved.getRewardBatchId());
                    assertEquals(SyncTrxStatus.REWARDED.name(), moved.getStatus());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED, moved.getRewardBatchTrxStatus());
                    assertNotNull(moved.getRewardBatchInclusionDate());
                    assertNotNull(moved.getUpdateDate());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        projectedBatch(SOURCE_BATCH_ID),
                        projectedBatch(TARGET_BATCH_ID),
                        transactionSnapshot("moved")
                ))
                .assertNext(result -> {
                    assertAggregate(result.getT1(), 1L, 50L, 0L, 0L, 0L, 0L, 50L);
                    assertAggregate(result.getT2(), 2L, 130L, 1L, 1L, 0L, 100L, 30L);
                    assertEquals(TARGET_BATCH_ID, result.getT3().batchId());
                    assertEquals(SyncTrxStatus.REWARDED.name(), result.getT3().syncStatus());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED.name(), result.getT3().batchStatus());
                    assertNotNull(result.getT3().inclusionDate());
                    assertNotNull(result.getT3().updateDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldCreateExactlyOneCreatedNextMonthTargetWithSourceGrouping() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, SOURCE_MONTH,
                                RewardBatchStatus.CREATED),
                        insertTransaction("moved", INITIATIVE_ID, MERCHANT_ID, SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 100L)
                ).then(adapter.postponeTransaction(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID,
                        "moved",
                        LocalDate.of(2026, Month.DECEMBER, 31)
                )))
                .assertNext(moved -> assertEquals("moved", moved.getId()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        targetGroupingCount(),
                        targetByGrouping(),
                        projectedBatch(SOURCE_BATCH_ID),
                        transactionSnapshot("moved")
                ))
                .assertNext(result -> {
                    assertEquals(1L, result.getT1());
                    RewardBatch target = result.getT2();
                    assertEquals(RewardBatchStatus.CREATED, target.getStatus());
                    assertEquals(INITIATIVE_ID, target.getInitiativeId());
                    assertEquals(MERCHANT_ID, target.getMerchantId());
                    assertEquals(TARGET_MONTH, target.getMonth());
                    assertEquals("Business", target.getBusinessName());
                    assertAggregate(result.getT3(), 0L, 0L, 0L, 0L, 0L, 0L, 0L);
                    assertEquals(target.getId(), result.getT4().batchId());
                })
                .verifyComplete();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("incompleteRequests")
    void shouldRejectIncompleteRequests(
            String ignored,
            String merchantId,
            String initiativeId,
            String sourceBatchId,
            String transactionId,
            LocalDate initiativeFruitionEndDate
    ) {
        StepVerifier.create(adapter.postponeTransaction(
                        merchantId,
                        initiativeId,
                        sourceBatchId,
                        transactionId,
                        initiativeFruitionEndDate
                ))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonCreatedBatches")
    void shouldRejectNonCreatedSourceOrTargetAtomically(
            String ignored,
            RewardBatchStatus sourceStatus,
            RewardBatchStatus targetStatus
    ) {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, SOURCE_MONTH, sourceStatus),
                        insertBatch(TARGET_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, TARGET_MONTH, targetStatus),
                        insertTransaction("protected", INITIATIVE_ID, MERCHANT_ID, SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 100L)
                ).then())
                .verifyComplete();

        assertStatusMismatch(adapter.postponeTransaction(
                MERCHANT_ID,
                INITIATIVE_ID,
                SOURCE_BATCH_ID,
                "protected",
                LocalDate.of(2026, Month.DECEMBER, 31)
        ));

        StepVerifier.create(Mono.zip(
                        transactionSnapshot("protected"),
                        projectedBatch(SOURCE_BATCH_ID, sourceStatus),
                        projectedBatch(TARGET_BATCH_ID, targetStatus)
                ))
                .assertNext(result -> {
                    assertEquals(SOURCE_BATCH_ID, result.getT1().batchId());
                    assertAggregate(result.getT2(), 1L, 100L, 0L, 0L, 0L, 0L, 100L);
                    assertAggregate(result.getT3(), 0L, 0L, 0L, 0L, 0L, 0L, 0L);
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectNextMonthAfterInitiativeFruitionLimitWithoutCreatingTarget() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, "2026-06",
                                RewardBatchStatus.CREATED),
                        insertTransaction("protected", INITIATIVE_ID, MERCHANT_ID, SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 100L)
                ).then())
                .verifyComplete();

        StepVerifier.create(adapter.postponeTransaction(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID,
                        "protected",
                        LocalDate.of(2026, Month.MAY, 31)
                ))
                .expectErrorSatisfies(error -> {
                    assertEquals(ClientExceptionWithBody.class, error.getClass());
                    assertEquals(
                            REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED,
                            ((ClientExceptionWithBody) error).getCode()
                    );
                })
                .verify();

        StepVerifier.create(Mono.zip(transactionSnapshot("protected"), groupingCount("2026-07")))
                .assertNext(result -> {
                    assertEquals(SOURCE_BATCH_ID, result.getT1().batchId());
                    assertEquals(0L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotMoveTransactionOutsideRequestedMerchantInitiativeAndSourceMembership() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, SOURCE_MONTH,
                                RewardBatchStatus.CREATED),
                        insertBatch("other-source", OTHER_INITIATIVE_ID, OTHER_MERCHANT_ID, SOURCE_MONTH,
                                RewardBatchStatus.CREATED),
                        insertBatch("source-with-other-merchant", INITIATIVE_ID, OTHER_MERCHANT_ID, SOURCE_MONTH,
                                RewardBatchStatus.CREATED),
                        insertTransaction("other-merchant", INITIATIVE_ID, OTHER_MERCHANT_ID, SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 100L),
                        insertTransaction("other-initiative", OTHER_INITIATIVE_ID, OTHER_MERCHANT_ID, "other-source",
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 200L),
                        insertTransaction("source-merchant-mismatch", INITIATIVE_ID, MERCHANT_ID,
                                "source-with-other-merchant", SyncTrxStatus.REWARDED,
                                RewardBatchTrxStatus.CONSULTABLE, 300L)
                ).then())
                .verifyComplete();

        assertTransactionNotFound(adapter.postponeTransaction(
                MERCHANT_ID,
                INITIATIVE_ID,
                SOURCE_BATCH_ID,
                "other-merchant",
                LocalDate.of(2026, Month.DECEMBER, 31)
        ));
        assertTransactionNotFound(adapter.postponeTransaction(
                OTHER_MERCHANT_ID,
                INITIATIVE_ID,
                SOURCE_BATCH_ID,
                "other-initiative",
                LocalDate.of(2026, Month.DECEMBER, 31)
        ));
        assertTransactionNotFound(adapter.postponeTransaction(
                MERCHANT_ID,
                INITIATIVE_ID,
                "source-with-other-merchant",
                "source-merchant-mismatch",
                LocalDate.of(2026, Month.DECEMBER, 31)
        ));

        StepVerifier.create(Mono.zip(
                        transactionSnapshot("other-merchant"),
                        transactionSnapshot("other-initiative"),
                        transactionSnapshot("source-merchant-mismatch"),
                        targetGroupingCount()
                ))
                .assertNext(result -> {
                    assertEquals(SOURCE_BATCH_ID, result.getT1().batchId());
                    assertEquals("other-source", result.getT2().batchId());
                    assertEquals("source-with-other-merchant", result.getT3().batchId());
                    assertEquals(0L, result.getT4());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotMoveTwiceOrCreateDuplicateTargetWhenRetriedConcurrently() {
        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE_BATCH_ID, INITIATIVE_ID, MERCHANT_ID, SOURCE_MONTH,
                                RewardBatchStatus.CREATED),
                        insertTransaction("moved", INITIATIVE_ID, MERCHANT_ID, SOURCE_BATCH_ID,
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 100L)
                ).then(adapter.postponeTransaction(
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID,
                        "moved",
                        LocalDate.of(2026, Month.DECEMBER, 31)
                ).then()))
                .verifyComplete();

        assertTransactionNotFound(adapter.postponeTransaction(
                MERCHANT_ID,
                INITIATIVE_ID,
                SOURCE_BATCH_ID,
                "moved",
                LocalDate.of(2026, Month.DECEMBER, 31)
        ));

        StepVerifier.create(retrySnapshot())
                .assertNext(snapshot -> {
                    assertEquals(1L, snapshot.targetGroupings());
                    assertEquals(0L, snapshot.sourceTransactions());
                    assertEquals(1L, snapshot.targetTransactions());
                })
                .verifyComplete();

        StepVerifier.create(Flux.concat(
                        insertBatch("concurrent-source", INITIATIVE_ID, MERCHANT_ID, "2026-07",
                                RewardBatchStatus.CREATED),
                        insertTransaction("concurrent", INITIATIVE_ID, MERCHANT_ID, "concurrent-source",
                                SyncTrxStatus.REWARDED, RewardBatchTrxStatus.CONSULTABLE, 75L)
                ).then(Mono.zip(
                        adapter.postponeTransaction(
                                        MERCHANT_ID,
                                        INITIATIVE_ID,
                                        "concurrent-source",
                                        "concurrent",
                                        LocalDate.of(2026, Month.DECEMBER, 31)
                                )
                                .materialize(),
                        adapter.postponeTransaction(
                                        MERCHANT_ID,
                                        INITIATIVE_ID,
                                        "concurrent-source",
                                        "concurrent",
                                        LocalDate.of(2026, Month.DECEMBER, 31)
                                )
                                .materialize()
                )))
                .assertNext(attempts -> assertConcurrentAttempts(attempts.getT1(), attempts.getT2()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        groupingCount("2026-08"),
                        transactionSnapshot("concurrent"),
                        batchMonthForTransaction("concurrent")
                ))
                .assertNext(result -> {
                    assertEquals(1L, result.getT1());
                    assertEquals("2026-08", result.getT3());
                })
                .verifyComplete();
    }

    private static Stream<Arguments> nonCreatedBatches() {
        return Stream.of(
                Arguments.of("source is SENT", RewardBatchStatus.SENT, RewardBatchStatus.CREATED),
                Arguments.of("target is SENT", RewardBatchStatus.CREATED, RewardBatchStatus.SENT)
        );
    }

    private static Stream<Arguments> incompleteRequests() {
        return Stream.of(
                Arguments.of(
                        "merchant is missing",
                        null,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID,
                        "transaction",
                        LocalDate.of(2026, Month.DECEMBER, 31)
                ),
                Arguments.of(
                        "initiative is blank",
                        MERCHANT_ID,
                        " ",
                        SOURCE_BATCH_ID,
                        "transaction",
                        LocalDate.of(2026, Month.DECEMBER, 31)
                ),
                Arguments.of(
                        "source batch is missing",
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        null,
                        "transaction",
                        LocalDate.of(2026, Month.DECEMBER, 31)
                ),
                Arguments.of(
                        "transaction is blank",
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID,
                        " ",
                        LocalDate.of(2026, Month.DECEMBER, 31)
                ),
                Arguments.of(
                        "fruition end date is missing",
                        MERCHANT_ID,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID,
                        "transaction",
                        null
                )
        );
    }

    private static Mono<Void> insertBatch(
            String batchId,
            String initiativeId,
            String merchantId,
            String month,
            RewardBatchStatus status
    ) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, business_name, month, pos_type, status, name,
                            assignee_level
                        )
                        VALUES (
                            :id, :initiativeId, :merchantId, 'Business', :month, 'PHYSICAL', :status, 'Batch',
                            'L1'
                        )
                        """)
                .bind("id", batchId)
                .bind("initiativeId", initiativeId)
                .bind("merchantId", merchantId)
                .bind("month", month)
                .bind("status", status.name())
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> insertTransaction(
            String transactionId,
            String initiativeId,
            String merchantId,
            String batchId,
            SyncTrxStatus syncStatus,
            RewardBatchTrxStatus batchStatus,
            long accruedRewardCents
    ) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, merchant_id, reward_batch_id, status,
                            reward_batch_trx_status, accrued_reward_cents
                        )
                        VALUES (
                            :transactionId, :initiativeId, :merchantId, :batchId, :syncStatus,
                            :batchStatus, :accruedRewardCents
                        )
                        """)
                .bind("transactionId", transactionId)
                .bind("initiativeId", initiativeId)
                .bind("merchantId", merchantId)
                .bind("batchId", batchId)
                .bind("syncStatus", syncStatus.name())
                .bind("batchStatus", batchStatus.name())
                .bind("accruedRewardCents", accruedRewardCents)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static void assertStatusMismatch(Mono<?> operation) {
        StepVerifier.create(operation)
                .expectErrorMatches(error -> error instanceof ClientException exception
                        && exception.getHttpStatus() == HttpStatus.BAD_REQUEST)
                .verify();
    }

    private static void assertTransactionNotFound(Mono<?> operation) {
        StepVerifier.create(operation)
                .expectErrorMatches(SqlMerchantTransactionPostponementAdapterTest::isTransactionNotFound)
                .verify();
    }

    private static void assertConcurrentAttempts(
            Signal<RewardTransaction> first,
            Signal<RewardTransaction> second
    ) {
        assertEquals(1L, Stream.of(first, second).filter(Signal::isOnNext).count());
        assertEquals(1L, Stream.of(first, second)
                .map(Signal::getThrowable)
                .filter(SqlMerchantTransactionPostponementAdapterTest::isTransactionNotFound)
                .count());
    }

    private static boolean isTransactionNotFound(Throwable error) {
        return error instanceof ClientExceptionNoBody exception
                && exception.getHttpStatus() == HttpStatus.NOT_FOUND;
    }

    private static Mono<RewardBatch> projectedBatch(String batchId) {
        return projectedBatch(batchId, RewardBatchStatus.CREATED);
    }

    private static Mono<RewardBatch> projectedBatch(String batchId, RewardBatchStatus status) {
        return listAdapter.findBatchesWithStatus(status, INITIATIVE_ID)
                .filter(batch -> batch.getId().equals(batchId))
                .single();
    }

    private static Mono<RewardBatch> targetByGrouping() {
        return listAdapter.findBatchesWithStatus(RewardBatchStatus.CREATED, INITIATIVE_ID)
                .filter(batch -> batch.getMonth().equals(TARGET_MONTH))
                .single();
    }

    private static Mono<Long> targetGroupingCount() {
        return groupingCount(TARGET_MONTH);
    }

    private static Mono<Long> groupingCount(String month) {
        return Mono.from(dslContext.selectCount()
                        .from(REWARD_BATCHES)
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(INITIATIVE_ID))
                        .and(REWARD_BATCHES.MERCHANT_ID.eq(MERCHANT_ID))
                        .and(REWARD_BATCHES.POS_TYPE.eq("PHYSICAL"))
                        .and(REWARD_BATCHES.MONTH.eq(month)))
                .map(result -> result.value1().longValue());
    }

    private static Mono<TransactionSnapshot> transactionSnapshot(String transactionId) {
        return Mono.from(dslContext.select(
                                REWARD_TRANSACTIONS.REWARD_BATCH_ID,
                                REWARD_TRANSACTIONS.STATUS,
                                REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                                REWARD_TRANSACTIONS.REWARD_BATCH_INCLUSION_DATE,
                                REWARD_TRANSACTIONS.UPDATE_DATE
                        )
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)))
                .map(SqlMerchantTransactionPostponementAdapterTest::toTransactionSnapshot);
    }

    private static Mono<String> batchMonthForTransaction(String transactionId) {
        return Mono.from(dslContext.select(REWARD_BATCHES.MONTH)
                        .from(REWARD_TRANSACTIONS)
                        .join(REWARD_BATCHES)
                        .on(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(REWARD_BATCHES.ID)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(
                                        REWARD_BATCHES.INITIATIVE_ID
                                )))
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)))
                .map(record -> record.get(REWARD_BATCHES.MONTH));
    }

    private static TransactionSnapshot toTransactionSnapshot(Record row) {
        return new TransactionSnapshot(
                row.get(REWARD_TRANSACTIONS.REWARD_BATCH_ID),
                row.get(REWARD_TRANSACTIONS.STATUS),
                row.get(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS),
                row.get(REWARD_TRANSACTIONS.REWARD_BATCH_INCLUSION_DATE),
                row.get(REWARD_TRANSACTIONS.UPDATE_DATE)
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

    private static Mono<RetrySnapshot> retrySnapshot() {
        return Mono.zip(
                        targetGroupingCount(),
                        transactionSnapshot("moved"),
                        projectedBatch(SOURCE_BATCH_ID),
                        targetByGrouping()
                )
                .map(result -> new RetrySnapshot(
                        result.getT1(),
                        result.getT2().batchId(),
                        result.getT3().getNumberOfTransactions(),
                        result.getT4().getNumberOfTransactions()
                ));
    }

    private record TransactionSnapshot(
            String batchId,
            String syncStatus,
            String batchStatus,
            LocalDateTime inclusionDate,
            LocalDateTime updateDate
    ) {
    }

    private record RetrySnapshot(
            long targetGroupings,
            String batchId,
            long sourceTransactions,
            long targetTransactions
    ) {
    }
}
