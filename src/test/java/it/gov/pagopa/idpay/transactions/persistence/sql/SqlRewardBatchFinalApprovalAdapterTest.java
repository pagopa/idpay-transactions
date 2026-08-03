package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
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
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchFinalApprovalAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE = "initiative-final";
    private static final String BATCH = "batch-final";
    private static SqlRewardBatchFinalApprovalAdapter adapter;
    private static SqlRewardBatchListAdapter listAdapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        RewardBatchSqlMapper mapper = new RewardBatchSqlMapper(JsonMapper.builder().build());
        adapter = new SqlRewardBatchFinalApprovalAdapter(transactionalOperator(), connectionFactory(), mapper);
        listAdapter = new SqlRewardBatchListAdapter(DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()), SQLDialect.POSTGRES), mapper);
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient().sql("DELETE FROM reward_transactions").fetch().rowsUpdated()
                .then(databaseClient().sql("DELETE FROM reward_batches").fetch().rowsUpdated())
                .block();
    }

    @Test
    void shouldApproveOnlyPendingRowsWithinBatchScopeAndKeepAggregateProjectionConsistentOnRetry() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH, INITIATIVE, RewardBatchStatus.APPROVING, RewardBatchAssignee.L3),
                        insertBatch("other-batch", INITIATIVE, RewardBatchStatus.APPROVING, RewardBatchAssignee.L3),
                        insertBatch("other-initiative-batch", "other-initiative", RewardBatchStatus.APPROVING,
                                RewardBatchAssignee.L3),
                        insertTransaction("to-check", BATCH, RewardBatchTrxStatus.TO_CHECK, 100),
                        insertTransaction("consultable", BATCH, RewardBatchTrxStatus.CONSULTABLE, 200),
                        insertTransaction("suspended", BATCH, RewardBatchTrxStatus.SUSPENDED, 300),
                        insertTransaction("rejected", BATCH, RewardBatchTrxStatus.REJECTED, 400),
                        insertTransaction("other-scope", "other-batch", RewardBatchTrxStatus.TO_CHECK, 500),
                        insertTransaction("other-initiative-scope", "other-initiative", "other-initiative-batch",
                                RewardBatchTrxStatus.TO_CHECK, 600)
                ).thenMany(Flux.concat(
                        adapter.prepareFinalApproval(BATCH, INITIATIVE),
                        adapter.prepareFinalApproval(BATCH, INITIATIVE)
                )))
                .assertNext(batch -> assertEquals(1L, batch.getNumberOfTransactionsSuspended()))
                .assertNext(batch -> assertEquals(1L, batch.getNumberOfTransactionsSuspended()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        transactionStatus("to-check"),
                        transactionStatus("consultable"),
                        transactionStatus("suspended"),
                        transactionStatus("rejected"),
                        transactionStatus("other-scope"),
                        transactionStatus("other-initiative-scope"),
                        listAdapter.findBatchesWithStatus(RewardBatchStatus.APPROVING, INITIATIVE)
                                .filter(batch -> BATCH.equals(batch.getId())).single()
                ))
                .assertNext(result -> {
                    assertEquals(RewardBatchTrxStatus.APPROVED.name(), result.getT1());
                    assertEquals(RewardBatchTrxStatus.APPROVED.name(), result.getT2());
                    assertEquals(RewardBatchTrxStatus.SUSPENDED.name(), result.getT3());
                    assertEquals(RewardBatchTrxStatus.REJECTED.name(), result.getT4());
                    assertEquals(RewardBatchTrxStatus.TO_CHECK.name(), result.getT5());
                    assertEquals(RewardBatchTrxStatus.TO_CHECK.name(), result.getT6());
                    RewardBatch projection = result.getT7();
                    assertEquals(4L, projection.getNumberOfTransactions());
                    assertEquals(4L, projection.getNumberOfTransactionsElaborated());
                    assertEquals(1L, projection.getNumberOfTransactionsSuspended());
                    assertEquals(1L, projection.getNumberOfTransactionsRejected());
                    assertEquals(300L, projection.getApprovedAmountCents());
                })
                .verifyComplete();
    }

    @Test
    void shouldCompleteEmptyApprovingL3BatchAndAllowApprovedRetry() {
        StepVerifier.create(insertBatch(BATCH, INITIATIVE, RewardBatchStatus.APPROVING, RewardBatchAssignee.L3)
                        .thenMany(Flux.concat(
                                adapter.completeFinalApproval(BATCH, INITIATIVE),
                                adapter.completeFinalApproval(BATCH, INITIATIVE)
                        )))
                .assertNext(batch -> assertEquals(RewardBatchStatus.APPROVED, batch.getStatus()))
                .assertNext(batch -> assertEquals(RewardBatchStatus.APPROVED, batch.getStatus()))
                .verifyComplete();

        StepVerifier.create(listAdapter.findBatchesWithStatus(RewardBatchStatus.APPROVED, INITIATIVE)
                        .filter(batch -> BATCH.equals(batch.getId()))
                        .single())
                .assertNext(batch -> {
                    assertEquals(0L, batch.getNumberOfTransactions());
                    assertEquals(0L, batch.getInitialAmountCents());
                    assertEquals(0L, batch.getApprovedAmountCents());
                })
                .verifyComplete();

        StepVerifier.create(insertBatch("wrong-assignee", INITIATIVE, RewardBatchStatus.APPROVING,
                        RewardBatchAssignee.L2)
                .then(adapter.completeFinalApproval("wrong-assignee", INITIATIVE)))
                .verifyComplete();

        StepVerifier.create(insertBatch("wrong-status", INITIATIVE, RewardBatchStatus.SENT, RewardBatchAssignee.L3)
                        .then(adapter.completeFinalApproval("wrong-status", INITIATIVE)))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatchesWithStatus(RewardBatchStatus.SENT, INITIATIVE)
                        .filter(batch -> "wrong-status".equals(batch.getId()))
                        .single())
                .assertNext(batch -> assertEquals(RewardBatchStatus.SENT, batch.getStatus()))
                .verifyComplete();
    }

    @Test
    void shouldRejectBlankIdentityBeforeFinalApprovalTransaction() {
        StepVerifier.create(adapter.prepareFinalApproval(" ", INITIATIVE))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch and initiative identifiers are required".equals(error.getMessage()))
                .verify();
        StepVerifier.create(adapter.completeFinalApproval(BATCH, " "))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch and initiative identifiers are required".equals(error.getMessage()))
                .verify();
        StepVerifier.create(adapter.prepareFinalApproval(null, INITIATIVE))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch and initiative identifiers are required".equals(error.getMessage()))
                .verify();
        StepVerifier.create(adapter.completeFinalApproval(BATCH, null))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch and initiative identifiers are required".equals(error.getMessage()))
                .verify();
    }

    private static Mono<Void> insertBatch(
            String id, String initiative, RewardBatchStatus status, RewardBatchAssignee assignee
    ) {
        return databaseClient().sql("""
                        INSERT INTO reward_batches (id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level)
                        VALUES (:id, :initiative, :merchant, '2026-07', 'PHYSICAL', :status, 'July', :assignee)
                        """)
                .bind("id", id).bind("initiative", initiative).bind("merchant", id).bind("status", status.name())
                .bind("assignee", assignee.name()).fetch().rowsUpdated().then();
    }

    private static Mono<Void> insertTransaction(
            String id, String batch, RewardBatchTrxStatus status, long amount
    ) {
        return insertTransaction(id, INITIATIVE, batch, status, amount);
    }

    private static Mono<Void> insertTransaction(
            String id, String initiative, String batch, RewardBatchTrxStatus status, long amount
    ) {
        return databaseClient().sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id, reward_batch_trx_status, accrued_reward_cents
                        ) VALUES (:id, :initiative, :batch, :status, :amount)
                        """)
                .bind("id", id).bind("initiative", initiative).bind("batch", batch)
                .bind("status", status.name()).bind("amount", amount).fetch().rowsUpdated().then();
    }

    private static Mono<String> transactionStatus(String id) {
        return Mono.from(DSL.using(new TransactionAwareConnectionFactoryProxy(connectionFactory()), SQLDialect.POSTGRES)
                        .select(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS).from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(id)))
                .map(row -> row.get(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS));
    }
}
