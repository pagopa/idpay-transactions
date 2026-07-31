package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.gov.pagopa.common.web.exception.BatchNotElaborated15PercentException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
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
class SqlRewardBatchAssigneePromotionAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE = "initiative-promotion";
    private static final String BATCH = "batch-promotion";
    private static SqlRewardBatchAssigneePromotionAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardBatchAssigneePromotionAdapter(
                transactionalOperator(), connectionFactory(),
                DSL.using(new TransactionAwareConnectionFactoryProxy(connectionFactory()), SQLDialect.POSTGRES),
                new RewardBatchSqlMapper(JsonMapper.builder().build())
        );
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
    void shouldPromoteL1AtExactTransactionDerivedFifteenPercentThenL2ToL3() {
        StepVerifier.create(insertBatch(BATCH, INITIATIVE, RewardBatchAssignee.L1)
                        .thenMany(Flux.concat(
                                Flux.range(1, 3).concatMap(index -> insertTransaction(
                                        "elaborated-" + index, BATCH, RewardBatchTrxStatus.APPROVED)),
                                Flux.range(1, 17).concatMap(index -> insertTransaction(
                                        "pending-" + index, BATCH, RewardBatchTrxStatus.CONSULTABLE))
                        ))
                        .then(adapter.promote(BATCH, INITIATIVE, RewardBatchAssignee.L1, RewardBatchAssignee.L2))
                        .flatMap(ignored -> adapter.promote(
                                BATCH, INITIATIVE, RewardBatchAssignee.L2, RewardBatchAssignee.L3
                        )))
                .assertNext(batch -> assertEquals(RewardBatchAssignee.L3, batch.getAssigneeLevel()))
                .verifyComplete();
    }

    @Test
    void shouldRejectBelowThresholdButPromoteAnEmptyBatchThroughL1ToL3() {
        StepVerifier.create(insertBatch(BATCH, INITIATIVE, RewardBatchAssignee.L1)
                        .thenMany(Flux.concat(
                                Flux.range(1, 2).concatMap(index -> insertTransaction(
                                        "below-" + index, BATCH, RewardBatchTrxStatus.APPROVED)),
                                Flux.range(1, 18).concatMap(index -> insertTransaction(
                                        "remaining-" + index, BATCH, RewardBatchTrxStatus.CONSULTABLE))
                        ))
                        .then(adapter.promote(BATCH, INITIATIVE, RewardBatchAssignee.L1, RewardBatchAssignee.L2)))
                .expectError(BatchNotElaborated15PercentException.class)
                .verify();

        StepVerifier.create(insertBatch("empty", INITIATIVE, RewardBatchAssignee.L1)
                        .then(adapter.promote("empty", INITIATIVE, RewardBatchAssignee.L1, RewardBatchAssignee.L2))
                        .flatMap(ignored -> adapter.promote(
                                "empty", INITIATIVE, RewardBatchAssignee.L2, RewardBatchAssignee.L3
                        )))
                .assertNext(batch -> assertEquals(RewardBatchAssignee.L3, batch.getAssigneeLevel()))
                .verifyComplete();

        StepVerifier.create(adapter.findBatchForPromotion(BATCH, "other-initiative"))
                .verifyComplete();
        StepVerifier.create(adapter.promote(BATCH, INITIATIVE, RewardBatchAssignee.L2, RewardBatchAssignee.L3))
                .verifyComplete();
    }

    private static Mono<Void> insertBatch(String id, String initiative, RewardBatchAssignee assignee) {
        return databaseClient().sql("""
                        INSERT INTO reward_batches (id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level)
                        VALUES (:id, :initiative, :merchant, '2026-07', 'PHYSICAL', 'EVALUATING', 'July', :assignee)
                        """)
                .bind("id", id).bind("initiative", initiative).bind("merchant", id).bind("assignee", assignee.name())
                .fetch().rowsUpdated().then();
    }

    private static Mono<Void> insertTransaction(String id, String batch, RewardBatchTrxStatus status) {
        return databaseClient().sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id, reward_batch_trx_status, accrued_reward_cents
                        ) VALUES (:id, :initiative, :batch, :status, 100)
                        """)
                .bind("id", id).bind("initiative", INITIATIVE).bind("batch", batch).bind("status", status.name())
                .fetch().rowsUpdated().then();
    }
}
