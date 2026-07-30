package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchEvaluationAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String MERCHANT_ID = "merchant-1";
    private static final String BATCH_ID = "batch-1";
    private static final String OTHER_BATCH_ID = "batch-2";
    private static final String BATCH_MONTH = "2026-07";

    private static SqlRewardBatchEvaluationAdapter adapter;
    private static SqlRewardBatchListAdapter listAdapter;
    private static DSLContext dslContext;
    private static RewardTransactionSqlMapper transactionMapper;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        dslContext = DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                SQLDialect.POSTGRES
        );
        RewardBatchSqlMapper batchMapper = new RewardBatchSqlMapper(JsonMapper.builder().build());
        transactionMapper = new RewardTransactionSqlMapper(JsonMapper.builder().build());
        adapter = new SqlRewardBatchEvaluationAdapter(
                transactionalOperator(),
                connectionFactory(),
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
    void shouldPrepareSentBatchUsingAllAssignedRowsForDeterministicSampling() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.SENT),
                        insertTransaction("sample-first", RewardBatchTrxStatus.CONSULTABLE, 1),
                        insertTransaction("sample-tie-a", RewardBatchTrxStatus.CONSULTABLE, 2),
                        insertTransaction("sample-tie-b", RewardBatchTrxStatus.CONSULTABLE, 2),
                        insertTransaction("remaining-first", RewardBatchTrxStatus.CONSULTABLE, 3),
                        insertTransaction("suspended", RewardBatchTrxStatus.SUSPENDED, 0),
                        insertTransactionWithoutBatchStatus("null-status", 1),
                        insertTransaction("remaining-second", RewardBatchTrxStatus.CONSULTABLE, 4),
                        insertTransaction("remaining-third", RewardBatchTrxStatus.CONSULTABLE, 5)
                ).then(adapter.prepareEvaluation(BATCH_ID, INITIATIVE_ID)))
                .assertNext(batch -> assertEquals(RewardBatchStatus.EVALUATING, batch.getStatus()))
                .verifyComplete();

        StepVerifier.create(batchTransactionStates(BATCH_ID))
                .assertNext(states -> {
                    assertEquals(8, states.size());
                    assertTrue(states.values().stream()
                            .allMatch(state -> state.syncStatus().equals(SyncTrxStatus.REWARDED.name())));
                    assertEquals(RewardBatchTrxStatus.TO_CHECK.name(),
                            states.get("null-status").batchStatus());
                    assertEquals(RewardBatchTrxStatus.TO_CHECK.name(),
                            states.get("sample-first").batchStatus());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE.name(),
                            states.get("sample-tie-a").batchStatus());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED.name(),
                            states.get("suspended").batchStatus());
                })
                .verifyComplete();

        StepVerifier.create(projectedBatch(BATCH_ID))
                .assertNext(batch -> {
                    assertEquals(8L, batch.getNumberOfTransactions());
                    assertEquals(800L, batch.getInitialAmountCents());
                    assertEquals(1L, batch.getNumberOfTransactionsElaborated());
                    assertEquals(1L, batch.getNumberOfTransactionsSuspended());
                    assertEquals(0L, batch.getNumberOfTransactionsRejected());
                    assertEquals(100L, batch.getSuspendedAmountCents());
                    assertEquals(700L, batch.getApprovedAmountCents());
                })
                .verifyComplete();
    }

    @Test
    void shouldPrepareEmptySentBatchWithoutSamplingRows() {
        StepVerifier.create(insertBatch(BATCH_ID, RewardBatchStatus.SENT)
                        .then(adapter.prepareEvaluation(BATCH_ID, INITIATIVE_ID)))
                .assertNext(batch -> assertEquals(RewardBatchStatus.EVALUATING, batch.getStatus()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        projectedBatch(BATCH_ID),
                        batchTransactionStates(BATCH_ID)
                ))
                .assertNext(result -> {
                    assertEquals(RewardBatchStatus.EVALUATING, result.getT1().getStatus());
                    assertTrue(result.getT2().isEmpty());
                })
                .verifyComplete();
    }

    @Test
    void shouldPrepareABatchAtMostOnceAcrossConcurrentRequests() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.SENT),
                        insertTransaction("transaction-1", RewardBatchTrxStatus.CONSULTABLE, 1),
                        insertTransaction("transaction-2", RewardBatchTrxStatus.CONSULTABLE, 2),
                        insertTransaction("transaction-3", RewardBatchTrxStatus.CONSULTABLE, 3),
                        insertTransaction("transaction-4", RewardBatchTrxStatus.CONSULTABLE, 4),
                        insertTransaction("transaction-5", RewardBatchTrxStatus.CONSULTABLE, 5),
                        insertTransaction("transaction-6", RewardBatchTrxStatus.CONSULTABLE, 6),
                        insertTransaction("transaction-7", RewardBatchTrxStatus.CONSULTABLE, 7),
                        insertTransaction("transaction-8", RewardBatchTrxStatus.CONSULTABLE, 8),
                        insertTransaction("transaction-9", RewardBatchTrxStatus.CONSULTABLE, 9),
                        insertTransaction("transaction-10", RewardBatchTrxStatus.CONSULTABLE, 10)
                ).thenMany(Flux.merge(
                        Mono.defer(() -> adapter.prepareEvaluation(BATCH_ID, INITIATIVE_ID)),
                        Mono.defer(() -> adapter.prepareEvaluation(BATCH_ID, INITIATIVE_ID))
                )).collectList())
                .assertNext(batches -> {
                    assertEquals(1, batches.size());
                    assertEquals(RewardBatchStatus.EVALUATING, batches.getFirst().getStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(BATCH_ID)
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                                        RewardBatchTrxStatus.TO_CHECK.name()
                                )))))
                .expectNextMatches(result -> result.value1() == 2)
                .verifyComplete();
    }

    @ParameterizedTest(name = "{0} to {1}")
    @MethodSource("decisionTransitions")
    void shouldProjectAggregateStateAfterEveryDecisionTransition(
            RewardBatchTrxStatus oldStatus,
            RewardBatchTrxStatus newStatus
    ) {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.EVALUATING),
                        insertTransaction("decision-transaction", oldStatus, 1)
                ).then(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        BATCH_ID,
                        "decision-transaction",
                        newStatus,
                        null,
                        BATCH_MONTH,
                        null
                )))
                .assertNext(previous -> {
                    assertEquals("decision-transaction", previous.getId());
                    assertEquals(oldStatus, previous.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        projectedBatch(BATCH_ID),
                        readTransaction("decision-transaction")
                ))
                .assertNext(result -> {
                    RewardBatch batch = result.getT1();
                    RewardTransaction transaction = result.getT2();
                    assertEquals(newStatus, transaction.getRewardBatchTrxStatus());
                    assertEquals(BATCH_MONTH, transaction.getRewardBatchLastMonthElaborated());
                    assertProjection(batch, newStatus);
                })
                .verifyComplete();
    }

    @Test
    void shouldUpdateTransactionWithNullInBatchStatus() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.EVALUATING),
                        insertTransactionWithoutBatchStatus("null-decision-status", 1)
                ).then(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        BATCH_ID,
                        "null-decision-status",
                        RewardBatchTrxStatus.APPROVED,
                        null,
                        BATCH_MONTH,
                        null
                )))
                .assertNext(previous -> {
                    assertEquals("null-decision-status", previous.getId());
                    assertNull(previous.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        projectedBatch(BATCH_ID),
                        readTransaction("null-decision-status")
                ))
                .assertNext(result -> {
                    assertEquals(RewardBatchTrxStatus.APPROVED, result.getT2().getRewardBatchTrxStatus());
                    assertEquals(BATCH_MONTH, result.getT2().getRewardBatchLastMonthElaborated());
                    assertProjection(result.getT1(), RewardBatchTrxStatus.APPROVED);
                })
                .verifyComplete();
    }

    @Test
    void shouldPreserveReasonHistoryAndOptionalChecksError() {
        ReasonDTO firstReason = new ReasonDTO(LocalDateTime.of(2026, Month.JULY, 1, 9, 0), "first");
        ReasonDTO secondReason = new ReasonDTO(LocalDateTime.of(2026, Month.JULY, 1, 10, 0), "second");
        ReasonDTO replacementReason = new ReasonDTO(
                LocalDateTime.of(2026, Month.JULY, 1, 11, 0),
                "replacement"
        );
        ChecksError checksError = new ChecksError(true, false, false, false, false, false, false, false);

        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.EVALUATING),
                        insertTransaction("reason-transaction", RewardBatchTrxStatus.REJECTED, 1)
                ).then(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        BATCH_ID,
                        "reason-transaction",
                        RewardBatchTrxStatus.REJECTED,
                        firstReason,
                        BATCH_MONTH,
                        checksError
                )).then(readTransaction("reason-transaction")))
                .assertNext(transaction -> {
                    assertEquals(List.of(firstReason), transaction.getRewardBatchRejectionReason());
                    assertEquals(checksError, transaction.getChecksError());
                })
                .verifyComplete();

        StepVerifier.create(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        BATCH_ID,
                        "reason-transaction",
                        RewardBatchTrxStatus.REJECTED,
                        secondReason,
                        BATCH_MONTH,
                        null
                ).then(readTransaction("reason-transaction")))
                .assertNext(transaction -> {
                    assertEquals(List.of(firstReason, secondReason), transaction.getRewardBatchRejectionReason());
                    assertNull(transaction.getChecksError());
                })
                .verifyComplete();

        StepVerifier.create(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        BATCH_ID,
                        "reason-transaction",
                        RewardBatchTrxStatus.APPROVED,
                        replacementReason,
                        BATCH_MONTH,
                        null
                ).then(readTransaction("reason-transaction")))
                .assertNext(transaction -> {
                    assertEquals(RewardBatchTrxStatus.APPROVED, transaction.getRewardBatchTrxStatus());
                    assertEquals(List.of(replacementReason), transaction.getRewardBatchRejectionReason());
                })
                .verifyComplete();
    }

    @Test
    void shouldLeaveRowsUntouchedForInvalidDecisionScope() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.SENT),
                        insertTransaction("sent-transaction", RewardBatchTrxStatus.CONSULTABLE, 1)
                ).then(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        BATCH_ID,
                        "sent-transaction",
                        RewardBatchTrxStatus.APPROVED,
                        null,
                        BATCH_MONTH,
                        null
                )))
                .verifyComplete();

        StepVerifier.create(readTransaction("sent-transaction"))
                .assertNext(transaction -> {
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, transaction.getRewardBatchTrxStatus());
                    assertNull(transaction.getRewardBatchLastMonthElaborated());
                })
                .verifyComplete();
    }

    @Test
    void shouldLeaveTransactionUntouchedOutsideDecisionBatchScope() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.EVALUATING),
                        insertBatch(OTHER_BATCH_ID, RewardBatchStatus.EVALUATING, "2026-08"),
                        insertTransaction("outside-scope-transaction", RewardBatchTrxStatus.CONSULTABLE, 1)
                ).then(adapter.updateStatusAndReturnOld(
                        INITIATIVE_ID,
                        OTHER_BATCH_ID,
                        "outside-scope-transaction",
                        RewardBatchTrxStatus.APPROVED,
                        null,
                        BATCH_MONTH,
                        null
                )))
                .verifyComplete();

        StepVerifier.create(readTransaction("outside-scope-transaction"))
                .assertNext(transaction -> {
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, transaction.getRewardBatchTrxStatus());
                    assertNull(transaction.getRewardBatchLastMonthElaborated());
                })
                .verifyComplete();
    }

    @Test
    void shouldSerializeConcurrentDecisionsForTheSameBatch() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH_ID, RewardBatchStatus.EVALUATING),
                        insertTransaction("concurrent-transaction", RewardBatchTrxStatus.TO_CHECK, 1)
                ).thenMany(Flux.merge(
                        Mono.defer(() -> adapter.updateStatusAndReturnOld(
                                INITIATIVE_ID,
                                BATCH_ID,
                                "concurrent-transaction",
                                RewardBatchTrxStatus.APPROVED,
                                null,
                                BATCH_MONTH,
                                null
                        )),
                        Mono.defer(() -> adapter.updateStatusAndReturnOld(
                                INITIATIVE_ID,
                                BATCH_ID,
                                "concurrent-transaction",
                                RewardBatchTrxStatus.APPROVED,
                                null,
                                BATCH_MONTH,
                                null
                        ))
                )).collectList())
                .assertNext(previous -> {
                    assertEquals(2, previous.size());
                    List<RewardBatchTrxStatus> oldStatuses = previous.stream()
                            .map(RewardTransaction::getRewardBatchTrxStatus)
                            .sorted(Comparator.comparing(Enum::name))
                            .toList();
                    assertEquals(List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.TO_CHECK), oldStatuses);
                })
                .verifyComplete();

        StepVerifier.create(projectedBatch(BATCH_ID))
                .assertNext(batch -> assertProjection(batch, RewardBatchTrxStatus.APPROVED))
                .verifyComplete();
    }

    private static Stream<Arguments> decisionTransitions() {
        return Stream.of(
                RewardBatchTrxStatus.APPROVED,
                RewardBatchTrxStatus.TO_CHECK,
                RewardBatchTrxStatus.CONSULTABLE,
                RewardBatchTrxStatus.SUSPENDED,
                RewardBatchTrxStatus.REJECTED
        ).flatMap(oldStatus -> Stream.of(
                RewardBatchTrxStatus.APPROVED,
                RewardBatchTrxStatus.REJECTED,
                RewardBatchTrxStatus.SUSPENDED
        ).map(newStatus -> Arguments.of(oldStatus, newStatus)));
    }

    private static void assertProjection(RewardBatch batch, RewardBatchTrxStatus status) {
        assertEquals(1L, batch.getNumberOfTransactions());
        assertEquals(100L, batch.getInitialAmountCents());
        switch (status) {
            case APPROVED -> {
                assertEquals(1L, batch.getNumberOfTransactionsElaborated());
                assertEquals(0L, batch.getNumberOfTransactionsSuspended());
                assertEquals(0L, batch.getNumberOfTransactionsRejected());
                assertEquals(0L, batch.getSuspendedAmountCents());
                assertEquals(100L, batch.getApprovedAmountCents());
            }
            case REJECTED -> {
                assertEquals(1L, batch.getNumberOfTransactionsElaborated());
                assertEquals(0L, batch.getNumberOfTransactionsSuspended());
                assertEquals(1L, batch.getNumberOfTransactionsRejected());
                assertEquals(0L, batch.getSuspendedAmountCents());
                assertEquals(0L, batch.getApprovedAmountCents());
            }
            case SUSPENDED -> {
                assertEquals(1L, batch.getNumberOfTransactionsElaborated());
                assertEquals(1L, batch.getNumberOfTransactionsSuspended());
                assertEquals(0L, batch.getNumberOfTransactionsRejected());
                assertEquals(100L, batch.getSuspendedAmountCents());
                assertEquals(0L, batch.getApprovedAmountCents());
            }
            default -> throw new IllegalArgumentException("Unsupported decision status " + status);
        }
    }

    private static Mono<Void> insertBatch(String batchId, RewardBatchStatus status) {
        return insertBatch(batchId, status, BATCH_MONTH);
    }

    private static Mono<Void> insertBatch(String batchId, RewardBatchStatus status, String month) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level
                        )
                        VALUES (
                            :id, :initiativeId, :merchantId, :month, 'PHYSICAL', :status, 'Luglio 2026', 'L1'
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

    private static Mono<Void> insertTransaction(
            String transactionId,
            RewardBatchTrxStatus batchStatus,
            int samplingKey
    ) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id, status,
                            reward_batch_trx_status, sampling_key, accrued_reward_cents
                        )
                        VALUES (
                            :id, :initiativeId, :batchId, :status, :batchStatus, :samplingKey, 100
                        )
                        """)
                .bind("id", transactionId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("batchId", BATCH_ID)
                .bind("status", SyncTrxStatus.INVOICED.name())
                .bind("batchStatus", batchStatus.name())
                .bind("samplingKey", samplingKey)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> insertTransactionWithoutBatchStatus(String transactionId, int samplingKey) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id, status,
                            sampling_key, accrued_reward_cents
                        )
                        VALUES (
                            :id, :initiativeId, :batchId, :status, :samplingKey, 100
                        )
                        """)
                .bind("id", transactionId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("batchId", BATCH_ID)
                .bind("status", SyncTrxStatus.INVOICED.name())
                .bind("samplingKey", samplingKey)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Map<String, TransactionState>> batchTransactionStates(String batchId) {
        return Flux.from(dslContext.select(
                                REWARD_TRANSACTIONS.TRANSACTION_ID,
                                REWARD_TRANSACTIONS.STATUS,
                                REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS
                        )
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(batchId)))
                .collectMap(
                        row -> row.get(REWARD_TRANSACTIONS.TRANSACTION_ID),
                        SqlRewardBatchEvaluationAdapterTest::transactionState
                );
    }

    private static TransactionState transactionState(Record3<String, String, String> row) {
        return new TransactionState(
                row.get(REWARD_TRANSACTIONS.STATUS),
                row.get(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS)
        );
    }

    private static Mono<RewardBatch> projectedBatch(String batchId) {
        return listAdapter.findBatchesWithStatus(RewardBatchStatus.EVALUATING, INITIATIVE_ID)
                .filter(batch -> batch.getId().equals(batchId))
                .single();
    }

    private static Mono<RewardTransaction> readTransaction(String transactionId) {
        return Mono.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)))
                .map(transactionMapper::fromRecord);
    }

    private record TransactionState(String syncStatus, String batchStatus) {
    }
}
