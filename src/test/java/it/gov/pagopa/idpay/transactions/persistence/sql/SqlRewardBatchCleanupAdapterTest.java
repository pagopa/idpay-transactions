package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.gov.pagopa.common.utils.CommonConstants;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.YearMonth;
import java.util.Set;
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

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchCleanupAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlRewardBatchCleanupAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardBatchCleanupAdapter(
                transactionalOperator(),
                DSL.using(new TransactionAwareConnectionFactoryProxy(connectionFactory()), SQLDialect.POSTGRES)
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
    void shouldDeleteOnlyOldUnreferencedBatchesAndRemainSafeOnRetry() {
        YearMonth current = YearMonth.now(CommonConstants.ZONEID);
        StepVerifier.create(Flux.concat(
                        insertBatch("old-empty", current.minusMonths(1)),
                        insertBatch("old-referenced", current.minusMonths(2)),
                        insertBatch("current", current),
                        insertBatch("future", current.plusMonths(1)),
                        insertTransaction("reference", "old-referenced")
                ).then(adapter.deleteEmptyBatches()).then(adapter.deleteEmptyBatches())
                        .thenMany(existingIds()))
                .assertNext(ids -> assertEquals(Set.of("old-referenced", "current", "future"), ids))
                .verifyComplete();
    }

    private static Mono<Void> insertBatch(String id, YearMonth month) {
        return databaseClient().sql("""
                        INSERT INTO reward_batches (id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level)
                        VALUES (:id, 'initiative-cleanup', :merchant, :month, 'PHYSICAL', 'CREATED', 'July', 'L1')
                        """)
                .bind("id", id).bind("merchant", id).bind("month", month.toString()).fetch().rowsUpdated().then();
    }

    private static Mono<Void> insertTransaction(String id, String batch) {
        return databaseClient().sql("""
                        INSERT INTO reward_transactions (transaction_id, initiative_id, reward_batch_id, accrued_reward_cents)
                        VALUES (:id, 'initiative-cleanup', :batch, 0)
                        """)
                .bind("id", id).bind("batch", batch).fetch().rowsUpdated().then();
    }

    private static Mono<Set<String>> existingIds() {
        return databaseClient().sql("SELECT id FROM reward_batches").map(
                (row, metadata) -> row.get("id", String.class)
        ).all().collect(java.util.stream.Collectors.toSet());
    }
}
