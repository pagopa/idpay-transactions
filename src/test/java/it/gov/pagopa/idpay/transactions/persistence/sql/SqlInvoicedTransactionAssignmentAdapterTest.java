package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.Map;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlInvoicedTransactionAssignmentAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String MERCHANT_ID = "merchant-1";

    private static SqlInvoicedTransactionAssignmentAdapter adapter;
    private static SqlRewardBatchAdapter batchAdapter;
    private static SqlRewardTransactionAdapter transactionAdapter;
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
        batchAdapter = new SqlRewardBatchAdapter(
                transactionalOperator(),
                dslContext,
                connectionFactory(),
                new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                        .getRepository(RewardBatchSqlRepository.class),
                batchMapper
        );
        transactionAdapter = new SqlRewardTransactionAdapter(
                transactionalOperator(),
                dslContext,
                transactionMapper
        );
        adapter = new SqlInvoicedTransactionAssignmentAdapter(
                transactionalOperator(),
                dslContext,
                connectionFactory(),
                batchAdapter,
                transactionAdapter,
                batchMapper,
                transactionMapper
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
    void shouldAssignAnEligibleTransactionAndClearPriorAssignmentMetadata() {
        RewardTransaction transaction = transaction("transaction-assigned", 750L);
        transaction.setRewardBatchRejectionReason(List.of(new ReasonDTO(null, "obsolete")));

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction, batch(), 123))
                .assertNext(assigned -> {
                    assertNotNull(assigned.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, assigned.getRewardBatchTrxStatus());
                    assertNotNull(assigned.getRewardBatchInclusionDate());
                    assertEquals(123, assigned.getSamplingKey());
                    assertNull(assigned.getRewardBatchRejectionReason());
                })
                .verifyComplete();

        StepVerifier.create(adapter.findInvoicedTransactionWithoutBatch(transaction.getId()))
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_BATCHES)
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(INITIATIVE_ID))))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
    }

    @Test
    void shouldNotCreateABatchWhenStaleInvoicedInputResolvesToRefundedTransaction() {
        RewardTransaction staleInvoiced = transaction("transaction-stale-refunded", 750L);
        staleInvoiced.setTransactionRevision(1L);

        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, status, accrued_reward_cents, transaction_revision
                                )
                                VALUES (
                                    'transaction-stale-refunded', 'initiative-1', 'REFUNDED', 750, 2
                                )
                                """)
                        .fetch()
                        .rowsUpdated()
                        .then(adapter.assignInvoicedTransaction(staleInvoiced, batch(), 123)))
                .assertNext(persisted -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), persisted.getStatus());
                    assertEquals(2L, persisted.getTransactionRevision());
                    assertNull(persisted.getRewardBatchId());
                    assertNull(persisted.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_BATCHES)))
                .expectNextMatches(result -> result.value1() == 0)
                .verifyComplete();
    }

    @Test
    void shouldNotClaimATransactionWhenANewerSnapshotIsNoLongerInvoiced() {
        RewardTransaction existing = transaction("transaction-no-claim", 750L);
        existing.setStatus(SyncTrxStatus.INVOICED.name());
        existing.setTransactionRevision(1L);
        RewardTransaction newer = transaction("transaction-no-claim", 750L);
        newer.setStatus(SyncTrxStatus.AUTHORIZED.name());
        newer.setTransactionRevision(2L);

        StepVerifier.create(transactionAdapter.upsert(existing)
                        .then(adapter.assignInvoicedTransaction(newer, batch(), 123)))
                .assertNext(persisted -> {
                    assertEquals(SyncTrxStatus.AUTHORIZED.name(), persisted.getStatus());
                    assertEquals(2L, persisted.getTransactionRevision());
                    assertNull(persisted.getRewardBatchId());
                    assertNull(persisted.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq("transaction-no-claim")
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.isNotNull()))))
                .expectNextMatches(result -> result.value1() == 0)
                .verifyComplete();
    }

    @Test
    void shouldAssignAnInvoicedSnapshotAfterAnExistingCapturedTransaction() {
        RewardTransaction captured = transaction("transaction-captured-to-invoiced", 750L);
        captured.setStatus("CAPTURED");
        captured.setTransactionRevision(1L);
        RewardTransaction invoiced = transaction("transaction-captured-to-invoiced", 750L);
        invoiced.setTransactionRevision(2L);

        StepVerifier.create(transactionAdapter.upsert(captured)
                        .then(adapter.assignInvoicedTransaction(invoiced, batch(), 123)))
                .assertNext(assigned -> {
                    assertNotNull(assigned.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, assigned.getRewardBatchTrxStatus());
                    assertEquals(2L, assigned.getTransactionRevision());
                })
                .verifyComplete();
    }

    @Test
    void shouldFindOnlyOrderedInvoicedTransactionsWithoutABatch() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, status, accrued_reward_cents
                                )
                                VALUES
                                    ('candidate-b', 'initiative-1', 'INVOICED', 0),
                                    ('candidate-a', 'initiative-1', 'INVOICED', 0),
                                    ('not-a-candidate', 'initiative-1', 'CANCELLED', 0)
                                """)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(adapter.findInvoicedTransactionsWithoutBatch(2)))
                .assertNext(transaction -> assertEquals("candidate-a", transaction.getId()))
                .assertNext(transaction -> assertEquals("candidate-b", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findInvoicedTransactionWithoutBatch("candidate-a"))
                .assertNext(transaction -> assertEquals("candidate-a", transaction.getId()))
                .verifyComplete();
        StepVerifier.create(adapter.findInvoicedTransactionWithoutBatch("not-a-candidate"))
                .verifyComplete();
    }

    @Test
    void shouldRejectABatchFromAnotherInitiativeBeforePersistingTheTransaction() {
        RewardBatch batch = batch();
        batch.setInitiativeId("other-initiative");

        StepVerifier.create(adapter.assignInvoicedTransaction(
                        transaction("transaction-mismatched-initiative", 750L),
                        batch,
                        123
                ))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains("same initiative"))
                .verify();

        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_TRANSACTIONS)))
                .expectNextMatches(result -> result.value1() == 0)
                .verifyComplete();
    }

    @Test
    void shouldRejectTransactionsWithoutExactlyOneNonBlankInitiative() {
        RewardTransaction withoutInitiative = transaction("transaction-without-initiative", 750L);
        withoutInitiative.setInitiatives(null);
        RewardTransaction withMultipleInitiatives = transaction("transaction-multiple-initiatives", 750L);
        withMultipleInitiatives.setInitiatives(List.of(INITIATIVE_ID, "initiative-2"));
        RewardTransaction withBlankInitiative = transaction("transaction-blank-initiative", 750L);
        withBlankInitiative.setInitiatives(List.of(" "));
        RewardBatch invalidBatch = batch();

        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withoutInitiative, invalidBatch, 123)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withMultipleInitiatives, invalidBatch, 123)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withBlankInitiative, invalidBatch, 123)
        );
    }

    @Test
    void shouldRejectAnAssignmentToANonCreatedBatchWithoutPersistingChanges() {
        RewardBatch batch = batch();
        batch.setId("sent-batch");
        batch.setStatus(RewardBatchStatus.SENT);

        StepVerifier.create(adapter.assignInvoicedTransaction(
                        transaction("transaction-sent-batch", 750L),
                        batch,
                        123
                ))
                .expectErrorMatches(error -> error instanceof ClientExceptionNoBody exception
                        && exception.getHttpStatus() == HttpStatus.BAD_REQUEST)
                .verify();

        StepVerifier.create(Mono.zip(
                        Mono.from(dslContext.selectCount().from(REWARD_TRANSACTIONS))
                                .map(Record1::value1),
                        Mono.from(dslContext.selectCount().from(REWARD_BATCHES))
                                .map(Record1::value1)
                ))
                .assertNext(counts -> {
                    assertEquals(0, counts.getT1());
                    assertEquals(0, counts.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRollbackNewBatchAndTransactionWhenMembershipClaimFails() {
        String functionName = "fail_invoiced_assignment_claim";
        String triggerName = "fail_invoiced_assignment_claim_trigger";

        try {
            StepVerifier.create(
                            databaseClient()
                                    .sql("""
                                            CREATE OR REPLACE FUNCTION fail_invoiced_assignment_claim()
                                            RETURNS trigger AS $$
                                            BEGIN
                                                IF NEW.reward_batch_id IS DISTINCT FROM OLD.reward_batch_id THEN
                                                    RAISE EXCEPTION 'forced invoiced assignment failure';
                                                END IF;
                                                RETURN NEW;
                                            END;
                                            $$ LANGUAGE plpgsql
                                            """)
                                    .then()
                                    .then(databaseClient()
                                            .sql("""
                                                    CREATE TRIGGER fail_invoiced_assignment_claim_trigger
                                                    BEFORE UPDATE ON reward_transactions
                                                    FOR EACH ROW
                                                    EXECUTE FUNCTION fail_invoiced_assignment_claim()
                                                    """)
                                            .then())
                                    .then(adapter.assignInvoicedTransaction(
                                            transaction("rollback-assignment", 750L),
                                            batch(),
                                            123
                                    )))
                    .expectErrorMatches(error -> error.getMessage() != null
                            && error.getMessage().contains("forced invoiced assignment failure"))
                    .verify();
        } finally {
            databaseClient()
                    .sql("DROP TRIGGER IF EXISTS " + triggerName + " ON reward_transactions")
                    .then()
                    .then(databaseClient().sql("DROP FUNCTION IF EXISTS " + functionName + "()").then())
                    .block();
        }

        StepVerifier.create(Mono.zip(
                        Mono.from(dslContext.selectCount().from(REWARD_BATCHES))
                                .map(Record1::value1),
                        Mono.from(dslContext.selectCount().from(REWARD_TRANSACTIONS))
                                .map(Record1::value1)
                ))
                .assertNext(counts -> {
                    assertEquals(0, counts.getT1());
                    assertEquals(0, counts.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldAssignUsingTheProvidedBatchIdentifier() {
        RewardBatch batch = batch();
        batch.setId("provided-batch-id");

        StepVerifier.create(adapter.assignInvoicedTransaction(
                        transaction("transaction-provided-batch", 750L),
                        batch,
                        123
                ))
                .assertNext(assigned -> assertEquals("provided-batch-id", assigned.getRewardBatchId()))
                .verifyComplete();
    }

    @Test
    void shouldKeepTheFirstAssignmentOnRetryAfterBatchLifecycleProgresses() {
        RewardTransaction original = transaction("transaction-retry", 750L);
        RewardTransaction retry = transaction("transaction-retry", 750L);

        StepVerifier.create(adapter.assignInvoicedTransaction(original, batch(), 11)
                        .flatMap(first -> Mono.from(dslContext.update(REWARD_BATCHES)
                                        .set(REWARD_BATCHES.STATUS, "SENT")
                                        .where(REWARD_BATCHES.ID.eq(first.getRewardBatchId())))
                                .then(adapter.assignInvoicedTransaction(retry, batch(), 99))
                                .map(second -> List.of(first, second))))
                .assertNext(assignments -> {
                    RewardTransaction first = assignments.getFirst();
                    RewardTransaction second = assignments.getLast();
                    assertEquals(first.getRewardBatchId(), second.getRewardBatchId());
                    assertEquals(11, second.getSamplingKey());
                    assertEquals(first.getRewardBatchInclusionDate(), second.getRewardBatchInclusionDate());
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq("transaction-retry"))))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_BATCHES)))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
    }

    @Test
    void shouldConcurrentlyAssignTransactionsToOneGroupingBatch() {
        RewardTransaction first = transaction("transaction-concurrent-1", 250L);
        RewardTransaction second = transaction("transaction-concurrent-2", 500L);

        StepVerifier.create(Flux.merge(
                        Mono.defer(() -> adapter.assignInvoicedTransaction(first, batch(), 1)),
                        Mono.defer(() -> adapter.assignInvoicedTransaction(second, batch(), 2))
                ).collectList())
                .assertNext(assignments -> {
                    assertEquals(2, assignments.size());
                    assertEquals(
                            1,
                            assignments.stream().map(RewardTransaction::getRewardBatchId).distinct().count()
                    );
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_BATCHES)))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                                RewardBatchTrxStatus.CONSULTABLE.name()
                        ))))
                .expectNextMatches(result -> result.value1() == 2)
                .verifyComplete();
        StepVerifier.create(Mono.from(dslContext.select(DSL.sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS))
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.isNotNull())))
                .expectNextMatches(result -> result.value1().longValue() == 750L)
                .verifyComplete();
    }

    @Test
    void shouldSerializeAssignmentToEarlierBatchAgainstLaterBatchSend() {
        RewardBatch earlier = batch();
        earlier.setId("concurrent-earlier");
        earlier.setMonth("2026-06");
        RewardBatch current = batch();
        current.setId("concurrent-current");
        current.setMonth("2026-07");
        RewardTransaction transaction = transaction("transaction-concurrent-send", 750L);

        StepVerifier.create(Flux.concat(
                        batchAdapter.createOrRead(earlier),
                        batchAdapter.createOrRead(current)
                )
                .thenMany(Flux.merge(
                        adapter.assignInvoicedTransaction(transaction, earlier, 123)
                                .thenReturn(true)
                                .onErrorReturn(false),
                        batchAdapter.sendBatch(
                                        current.getId(),
                                        current.getInitiativeId(),
                                        current.getMerchantId()
                                )
                                .thenReturn(true)
                                .onErrorReturn(false)
                ))
                .collectList())
                .assertNext(results -> assertEquals(1, results.stream()
                        .filter(Boolean::booleanValue)
                        .count()))
                .verifyComplete();
    }

    @Test
    void shouldRejectAssignmentToEarlierBatchAfterLaterBatchWasSent() {
        RewardBatch earlier = batch();
        earlier.setId("sent-first-earlier");
        earlier.setMonth("2026-06");
        RewardBatch current = batch();
        current.setId("sent-first-current");
        current.setMonth("2026-07");

        StepVerifier.create(Flux.concat(
                        batchAdapter.createOrRead(earlier),
                        batchAdapter.createOrRead(current)
                )
                .then(batchAdapter.sendBatch(
                        current.getId(),
                        current.getInitiativeId(),
                        current.getMerchantId()
                ))
                .then(adapter.assignInvoicedTransaction(
                        transaction("transaction-after-send", 750L),
                        earlier,
                        123
                )))
                .expectErrorMatches(error -> error instanceof ClientExceptionNoBody
                        && ((ClientExceptionNoBody) error).getHttpStatus() == HttpStatus.BAD_REQUEST)
                .verify();

        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq("transaction-after-send")
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.isNotNull()))))
                .expectNextMatches(result -> result.value1() == 0)
                .verifyComplete();
    }

    @Test
    void shouldAcquireBatchLockBeforeUpdatingAnUnassignedTransaction() {
        String batchId = "assignment-lock-batch";
        String transactionId = "assignment-lock-transaction";
        RewardBatch targetBatch = batch();
        targetBatch.setId(batchId);
        RewardTransaction transaction = transaction(transactionId, 750L);
        transaction.setStatus(SyncTrxStatus.INVOICED.name());

        StepVerifier.create(batchAdapter.createOrRead(targetBatch)
                        .then(transactionAdapter.upsert(transaction))
                        .then(SqlBatchLockTestSupport.holdBatch(
                                connectionFactory(),
                                batchId,
                                INITIATIVE_ID
                        ).flatMap(batchLock -> {
                            var pending = adapter.assignInvoicedTransaction(
                                    transaction,
                                    targetBatch,
                                    123
                            ).toFuture();
                            return SqlBatchLockTestSupport.blockedByWithin(
                                            connectionFactory(),
                                            batchLock.backendPid()
                                    )
                                    .flatMap(blocked -> {
                                        Mono<Void> evidence = blocked
                                                ? persistedAssignmentState(transactionId)
                                                        .doOnNext(state -> {
                                                            assertNull(state.batchId());
                                                            assertNull(state.batchStatus());
                                                        })
                                                        .then()
                                                : Mono.empty();
                                        return evidence
                                                .then(batchLock.release())
                                                .then(Mono.fromFuture(pending))
                                                .map(assigned -> {
                                                    assertTrue(blocked,
                                                            "assignment must wait for the batch row lock");
                                                    return assigned;
                                                });
                                    })
                                    .onErrorResume(error -> batchLock.release().then(Mono.error(error)));
                        })))
                .assertNext(assigned -> {
                    assertEquals(batchId, assigned.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, assigned.getRewardBatchTrxStatus());
                    assertEquals(123, assigned.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldCaptureCommittedAssignmentInTheSendSnapshot() {
        String batchId = "assignment-before-send";
        String transactionId = "assignment-before-send-transaction";
        RewardBatch targetBatch = batch();
        targetBatch.setId(batchId);
        RewardTransaction transaction = transaction(transactionId, 750L);

        StepVerifier.create(batchAdapter.createOrRead(targetBatch)
                        .then(transactionAdapter.upsert(transaction))
                        .then())
                .verifyComplete();

        StepVerifier.create(SqlBatchLockTestSupport
                        .holdTransaction(connectionFactory(), transactionId, INITIATIVE_ID)
                        .flatMap(transactionLock -> {
                            var pendingAssignment = adapter.assignInvoicedTransaction(
                                    transaction,
                                    targetBatch,
                                    123
                            ).toFuture();
                            return SqlBatchLockTestSupport.blockedByWithin(
                                            connectionFactory(),
                                            transactionLock.backendPid()
                                    )
                                    .flatMap(blocked -> {
                                        assertTrue(
                                                blocked,
                                                "assignment must reach the transaction row after acquiring the batch row"
                                        );
                                        var pendingSend = batchAdapter.sendBatch(
                                                batchId,
                                                INITIATIVE_ID,
                                                MERCHANT_ID
                                        ).toFuture();
                                        return transactionLock.release()
                                                .then(Mono.fromFuture(pendingAssignment))
                                                .then(Mono.fromFuture(pendingSend));
                                    })
                                    .onErrorResume(error -> transactionLock.release()
                                            .then(Mono.error(error)));
                        })
                        .timeout(Duration.ofSeconds(15)))
                .assertNext(sent -> assertEquals(RewardBatchStatus.SENT, sent.getStatus()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        persistedAssignmentState(transactionId),
                        databaseClient()
                                .sql("""
                                        SELECT initial_amount_cents_at_send
                                        FROM reward_batches
                                        WHERE id = :batchId
                                        """)
                                .bind("batchId", batchId)
                                .map((row, metadata) -> row.get(
                                        "initial_amount_cents_at_send",
                                        Long.class
                                ))
                                .one()
                ))
                .assertNext(result -> {
                    assertEquals(new AssignmentState(batchId, RewardBatchTrxStatus.CONSULTABLE.name()),
                            result.getT1());
                    assertEquals(750L, result.getT2());
                })
                .verifyComplete();
    }

    private static Mono<AssignmentState> persistedAssignmentState(String transactionId) {
        return databaseClient()
                .sql("""
                        SELECT reward_batch_id, reward_batch_trx_status
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", transactionId)
                .map((row, metadata) -> new AssignmentState(
                        row.get("reward_batch_id", String.class),
                        row.get("reward_batch_trx_status", String.class)
                ))
                .one();
    }

    private record AssignmentState(String batchId, String batchStatus) {
    }

    private static RewardBatch batch() {
        return RewardBatchFactory.create(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07",
                "Business"
        );
    }

    private static RewardTransaction transaction(String id, long accruedRewardCents) {
        return RewardTransaction.builder()
                .id(id)
                .initiatives(List.of(INITIATIVE_ID))
                .merchantId(MERCHANT_ID)
                .pointOfSaleId("pos-1")
                .pointOfSaleType(PosType.PHYSICAL)
                .businessName("Business")
                .status(SyncTrxStatus.INVOICED.name())
                .trxChargeDate(LocalDateTime.of(2026, Month.JULY, 1, 10, 30))
                .invoiceUploadDate(LocalDateTime.of(2026, Month.JULY, 1, 10, 30))
                .rewards(Map.of(INITIATIVE_ID, Reward.builder()
                        .accruedRewardCents(accruedRewardCents)
                        .build()))
                .build();
    }
}
