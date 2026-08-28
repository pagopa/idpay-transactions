package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.Map;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardTransactionAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlRewardTransactionAdapter adapter;
    private static SqlInvoicedTransactionAssignmentAdapter assignmentAdapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        var dslContext = DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                SQLDialect.POSTGRES
        );
        var jsonMapper = JsonMapper.builder().build();
        var transactionMapper = new RewardTransactionSqlMapper(jsonMapper);
        adapter = new SqlRewardTransactionAdapter(
                transactionalOperator(),
                dslContext,
                transactionMapper
        );
        assignmentAdapter = new SqlInvoicedTransactionAssignmentAdapter(
                transactionalOperator(),
                dslContext,
                connectionFactory(),
                new SqlRewardBatchAdapter(
                        transactionalOperator(),
                        dslContext,
                        new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                                .getRepository(RewardBatchSqlRepository.class),
                        new RewardBatchSqlMapper(jsonMapper)
                ),
                adapter,
                new RewardBatchSqlMapper(jsonMapper),
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
    void shouldRoundTripJsonAndDeriveAccruedRewardForTheTransactionInitiative() {
        RewardTransaction transaction = transaction("transaction-json", "initiative-1");
        RewardTransactionSqlMapper mapper = new RewardTransactionSqlMapper(JsonMapper.builder().build());

        RewardTransaction restored = mapper.fromEntity(mapper.toEntity(transaction));

        assertEquals(List.of("initiative-1"), restored.getInitiatives());
        assertEquals(transaction.getRewards(), restored.getRewards());
        assertEquals(transaction.getInitiativeRejectionReasons(), restored.getInitiativeRejectionReasons());
        assertEquals(transaction.getRewardBatchRejectionReason(), restored.getRewardBatchRejectionReason());
        assertEquals(750L, mapper.toEntity(transaction).accruedRewardCents());
    }

    @Test
    void shouldRejectTransactionsWithoutExactlyOneInitiative() {
        RewardTransaction transaction = transaction("transaction-invalid", "initiative-1");
        transaction.setInitiatives(List.of("initiative-1", "initiative-2"));
        RewardTransactionSqlMapper mapper = new RewardTransactionSqlMapper(JsonMapper.builder().build());

        assertThrows(IllegalArgumentException.class, () -> mapper.toEntity(transaction));
    }

    @Test
    void shouldIdempotentlyUpdateTransactionWithinItsInitiative() {
        RewardTransaction first = transaction("transaction-upsert", "initiative-1");
        first.setTransactionRevision(1L);
        RewardTransaction retry = transaction("transaction-upsert", "initiative-1");
        retry.setTransactionRevision(2L);
        retry.setStatus("INVOICED");
        retry.setAmountCents(2_000L);
        retry.setRewards(Map.of("initiative-1", Reward.builder().accruedRewardCents(1_100L).build()));

        StepVerifier.create(adapter.upsert(first)
                        .then(adapter.upsert(retry)))
                .assertNext(saved -> {
                    assertEquals("INVOICED", saved.getStatus());
                    assertEquals(2_000L, saved.getAmountCents());
                    assertEquals(1_100L, saved.getRewards().get("initiative-1").getAccruedRewardCents());
                    assertEquals(2L, saved.getTransactionRevision());
                })
                .verifyComplete();

        StepVerifier.create(databaseClient()
                        .sql("SELECT COUNT(*) AS count FROM reward_transactions")
                        .map((row, metadata) -> row.get("count", Long.class))
                        .one())
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void shouldApplyOnlyNewerGenericRevisionsWithoutOverwritingLocalMembership() {
        RewardTransaction original = transaction("transaction-revision", "initiative-1");
        original.setTransactionRevision(1L);
        RewardTransaction newer = transaction("transaction-revision", "initiative-1");
        newer.setTransactionRevision(2L);
        newer.setStatus("INVOICED");
        newer.setAmountCents(2_000L);
        RewardTransaction sameRevision = transaction("transaction-revision", "initiative-1");
        sameRevision.setTransactionRevision(2L);
        sameRevision.setStatus("REFUNDED");
        RewardTransaction older = transaction("transaction-revision", "initiative-1");
        older.setTransactionRevision(1L);
        older.setStatus("CANCELLED");

        StepVerifier.create(adapter.upsert(original)
                        .then(databaseClient()
                                .sql("""
                                        INSERT INTO reward_batches (
                                            id, initiative_id, merchant_id, month, pos_type,
                                            status, name, assignee_level
                                        )
                                        VALUES (
                                            'revision-batch', 'initiative-1', 'merchant', '2026-07',
                                            'PHYSICAL', 'CREATED', 'July', 'L1'
                                        )
                                        """)
                                .fetch()
                                .rowsUpdated())
                        .then(databaseClient()
                                .sql("""
                                        UPDATE reward_transactions
                                        SET reward_batch_id = 'revision-batch',
                                            reward_batch_trx_status = 'CONSULTABLE',
                                            reward_batch_inclusion_date = TIMESTAMP '2026-07-01 10:00:00',
                                            sampling_key = 77
                                        WHERE transaction_id = 'transaction-revision'
                                        """)
                                .fetch()
                                .rowsUpdated())
                        .then(adapter.upsert(newer))
                        .then(adapter.upsert(sameRevision))
                        .then(adapter.upsert(older)))
                .assertNext(saved -> {
                    assertEquals("INVOICED", saved.getStatus());
                    assertEquals(2L, saved.getTransactionRevision());
                    assertEquals("revision-batch", saved.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, saved.getRewardBatchTrxStatus());
                    assertEquals(77, saved.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldKeepGenericUpsertStatusAgnosticForANewerRefundedSnapshot() {
        RewardTransaction invoiced = invoicedTransaction(
                "transaction-generic-refunded",
                "initiative-1",
                1L
        );
        RewardTransaction refunded = transaction("transaction-generic-refunded", "initiative-1");
        refunded.setTransactionRevision(2L);
        refunded.setStatus(SyncTrxStatus.REFUNDED.name());

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                invoiced,
                                batch("generic-refunded-batch"),
                                77
                        )
                        .then(adapter.upsert(refunded)))
                .assertNext(saved -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), saved.getStatus());
                    assertEquals("generic-refunded-batch", saved.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, saved.getRewardBatchTrxStatus());
                    assertEquals(77, saved.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldDetachAssignedMembershipWhenPersistingANewerRefundedSnapshot() {
        RewardTransaction invoiced = invoicedTransaction("transaction-refunded-detach", "initiative-1", 1L);
        ChecksError checksError = new ChecksError(
                true, false, false, false, false, false, false, false
        );
        invoiced.setChecksError(checksError);
        RewardTransaction refunded = transaction("transaction-refunded-detach", "initiative-1");
        refunded.setTransactionRevision(2L);
        refunded.setStatus(SyncTrxStatus.REFUNDED.name());

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                invoiced,
                                batch("refunded-detach-batch"),
                                77
                        )
                        .then(adapter.upsertRefundedAndDetach(refunded)))
                .assertNext(saved -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), saved.getStatus());
                    assertEquals(2L, saved.getTransactionRevision());
                    assertNull(saved.getRewardBatchId());
                    assertNull(saved.getRewardBatchTrxStatus());
                    assertNull(saved.getRewardBatchInclusionDate());
                    assertEquals(0, saved.getSamplingKey());
                    assertEquals(checksError, saved.getChecksError());
                })
                .verifyComplete();
    }

    @Test
    void shouldPersistRefundedStatusForAnAlreadyUnassignedTransaction() {
        RewardTransaction original = transaction("transaction-refunded-unassigned", "initiative-1");
        original.setTransactionRevision(1L);
        original.setRewardBatchTrxStatus(null);
        original.setSamplingKey(0);
        RewardTransaction refunded = transaction("transaction-refunded-unassigned", "initiative-1");
        refunded.setTransactionRevision(2L);
        refunded.setStatus(SyncTrxStatus.REFUNDED.name());
        refunded.setRewardBatchTrxStatus(null);
        refunded.setSamplingKey(0);

        StepVerifier.create(adapter.upsert(original)
                        .then(adapter.upsertRefundedAndDetach(refunded)))
                .assertNext(saved -> {
                    assertEquals("REFUNDED", saved.getStatus());
                    assertNull(saved.getRewardBatchId());
                    assertNull(saved.getRewardBatchTrxStatus());
                    assertEquals(0, saved.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldIgnoreARetryOfAnAlreadyAppliedRefundedSnapshot() {
        RewardTransaction invoiced = invoicedTransaction("transaction-refunded-retry", "initiative-1", 1L);
        RewardTransaction refunded = transaction("transaction-refunded-retry", "initiative-1");
        refunded.setTransactionRevision(2L);
        refunded.setStatus(SyncTrxStatus.REFUNDED.name());
        refunded.setAmountCents(2_000L);
        RewardTransaction retry = transaction("transaction-refunded-retry", "initiative-1");
        retry.setTransactionRevision(2L);
        retry.setStatus(SyncTrxStatus.REFUNDED.name());
        retry.setAmountCents(9_999L);

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                invoiced,
                                batch("refunded-retry-batch"),
                                77
                        )
                        .then(adapter.upsertRefundedAndDetach(refunded))
                        .then(adapter.upsertRefundedAndDetach(retry)))
                .assertNext(saved -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), saved.getStatus());
                    assertEquals(2L, saved.getTransactionRevision());
                    assertEquals(2_000L, saved.getAmountCents());
                    assertNull(saved.getRewardBatchId());
                    assertNull(saved.getRewardBatchTrxStatus());
                    assertNull(saved.getRewardBatchInclusionDate());
                    assertEquals(0, saved.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldIgnoreAStaleRefundedSnapshotAgainstANewerRevision() {
        RewardTransaction invoiced = invoicedTransaction("transaction-refunded-stale", "initiative-1", 1L);
        RewardTransaction newer = transaction("transaction-refunded-stale", "initiative-1");
        newer.setTransactionRevision(3L);
        newer.setStatus(SyncTrxStatus.INVOICED.name());
        newer.setAmountCents(2_000L);
        RewardTransaction staleRefunded = transaction("transaction-refunded-stale", "initiative-1");
        staleRefunded.setTransactionRevision(2L);
        staleRefunded.setStatus(SyncTrxStatus.REFUNDED.name());
        staleRefunded.setAmountCents(9_999L);

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                invoiced,
                                batch("refunded-stale-batch"),
                                77
                        )
                        .then(adapter.upsert(newer))
                        .then(adapter.upsertRefundedAndDetach(staleRefunded)))
                .assertNext(saved -> {
                    assertEquals(SyncTrxStatus.INVOICED.name(), saved.getStatus());
                    assertEquals(3L, saved.getTransactionRevision());
                    assertEquals(2_000L, saved.getAmountCents());
                    assertEquals("refunded-stale-batch", saved.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, saved.getRewardBatchTrxStatus());
                    assertEquals(77, saved.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectARefundedSnapshotThatChangesTheExistingTransactionInitiative() {
        RewardTransaction invoiced = invoicedTransaction("transaction-refunded-initiative", "initiative-1", 1L);
        RewardTransaction conflicting = transaction("transaction-refunded-initiative", "initiative-2");
        conflicting.setTransactionRevision(2L);
        conflicting.setStatus(SyncTrxStatus.REFUNDED.name());

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                invoiced,
                                batch("refunded-initiative-batch"),
                                77
                        )
                        .then(adapter.upsertRefundedAndDetach(conflicting)))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("initiative-1"))
                .verify();

        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT initiative_id, status, reward_batch_id, sampling_key
                                FROM reward_transactions
                                WHERE transaction_id = 'transaction-refunded-initiative'
                                """)
                        .map((row, metadata) -> row.get("initiative_id", String.class)
                                + ":" + row.get("status", String.class)
                                + ":" + row.get("reward_batch_id", String.class)
                                + ":" + row.get("sampling_key", Integer.class))
                        .one())
                .expectNext("initiative-1:INVOICED:refunded-initiative-batch:77")
                .verifyComplete();
    }

    @Test
    void shouldInsertAFirstSeenRefundedTransactionWithoutLocalBatchState() {
        RewardTransaction refunded = transaction("transaction-refunded-new", "initiative-1");
        refunded.setTransactionRevision(1L);
        refunded.setStatus(SyncTrxStatus.REFUNDED.name());
        refunded.setRewardBatchId("payload-batch");
        refunded.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
        refunded.setRewardBatchInclusionDate(LocalDateTime.of(2026, Month.JULY, 2, 10, 30));
        refunded.setRewardBatchLastMonthElaborated("2026-06");
        refunded.setSamplingKey(77);
        refunded.setChecksError(new ChecksError(
                true, false, false, false, false, false, false, false
        ));

        StepVerifier.create(adapter.upsertRefundedAndDetach(refunded))
                .assertNext(saved -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), saved.getStatus());
                    assertNull(saved.getRewardBatchId());
                    assertNull(saved.getRewardBatchTrxStatus());
                    assertNull(saved.getRewardBatchInclusionDate());
                    assertNull(saved.getRewardBatchLastMonthElaborated());
                    assertNull(saved.getRewardBatchRejectionReason());
                    assertNull(saved.getChecksError());
                    assertEquals(0, saved.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectDetachOperationForANonRefundedTransaction() {
        RewardTransaction invoiced = invoicedTransaction(
                "transaction-invalid-detach",
                "initiative-1",
                1L
        );

        StepVerifier.create(adapter.upsertRefundedAndDetach(invoiced))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains("REFUNDED"))
                .verify();
    }

    @Test
    void shouldRejectAnUpsertThatChangesTheExistingTransactionInitiative() {
        RewardTransaction original = transaction("transaction-initiative", "initiative-1");
        original.setTransactionRevision(1L);
        RewardTransaction conflicting = transaction("transaction-initiative", "initiative-2");
        conflicting.setTransactionRevision(2L);
        conflicting.setStatus("INVOICED");

        StepVerifier.create(adapter.upsert(original)
                        .then(adapter.upsert(conflicting)))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("initiative-1"))
                .verify();

        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT initiative_id, status
                                FROM reward_transactions
                                WHERE transaction_id = 'transaction-initiative'
                                """)
                        .map((row, metadata) -> row.get("initiative_id", String.class)
                                + ":" + row.get("status", String.class))
                        .one())
                .expectNext("initiative-1:AUTHORIZED")
                .verifyComplete();
    }

    private static RewardBatch batch(String batchId) {
        RewardBatch batch = RewardBatchFactory.create(
                "initiative-1",
                "merchant",
                PosType.PHYSICAL,
                "2026-07",
                "Business"
        );
        batch.setId(batchId);
        return batch;
    }

    private static RewardTransaction invoicedTransaction(String id, String initiativeId, long revision) {
        RewardTransaction transaction = transaction(id, initiativeId);
        transaction.setStatus(SyncTrxStatus.INVOICED.name());
        transaction.setTransactionRevision(revision);
        return transaction;
    }

    private static RewardTransaction transaction(String id, String initiativeId) {
        return RewardTransaction.builder()
                .id(id)
                .initiatives(List.of(initiativeId))
                .idTrxAcquirer("acquirer-transaction")
                .acquirerCode("acquirer-code")
                .trxDate(LocalDateTime.of(2026, Month.JULY, 1, 10, 30))
                .operationType("PAYMENT")
                .circuitType("VISA")
                .idTrxIssuer("issuer-transaction")
                .correlationId("correlation")
                .amountCents(1_000L)
                .amountCurrency("EUR")
                .acquirerId("acquirer")
                .merchantId("merchant")
                .pointOfSaleId("pos")
                .posType("PHYSICAL")
                .status("AUTHORIZED")
                .rejectionReasons(List.of("reason"))
                .initiativeRejectionReasons(Map.of(initiativeId, List.of("initiative-reason")))
                .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(750L).build()))
                .userId("user")
                .additionalProperties(Map.of("property", "value"))
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewardBatchRejectionReason(List.of(new ReasonDTO(null, "batch-reason")))
                .pointOfSaleType(PosType.PHYSICAL)
                .samplingKey(123)
                .extendedAuthorization(true)
                .build();
    }
}
