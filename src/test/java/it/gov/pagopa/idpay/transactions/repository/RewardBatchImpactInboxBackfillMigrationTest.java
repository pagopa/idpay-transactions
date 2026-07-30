package it.gov.pagopa.idpay.transactions.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Exercises the upgrade path performed by migration 006: seeds representative
 * {@code reward_batch_impact_inbox} rows (and matching transactions) under the schema left by
 * migration 005, then applies migration 006 on top of that seeded state and asserts that:
 * <ul>
 *     <li>each transaction's {@code latest_applied_payment_impact_revision} is backfilled with
 *     the {@code MAX(transaction_revision)} recorded for it in the inbox, not merely the last
 *     inserted row;</li>
 *     <li>a transaction with no inbox rows at all is left at its column default (0), rather than
 *     being backfilled or dropped;</li>
 *     <li>the {@code reward_batch_impact_inbox} table itself is dropped afterwards.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
class RewardBatchImpactInboxBackfillMigrationTest extends PostgresqlMigrationTestSupport {

    @BeforeAll
    static void setUpDatabaseUpToInboxCreation() {
        applyRepositoryMigrationsUpToAndIncluding("005-add-payment-batch-impact-inbox.sql");
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @Test
    void shouldBackfillLatestAppliedPaymentImpactRevisionFromInboxMaxAndDropTheInboxTable() {
        StepVerifier.create(seedTransaction("backfill-multiple-events")
                        .then(seedTransaction("backfill-single-event"))
                        .then(seedTransaction("backfill-no-events"))
                        // Out-of-order inserts on purpose: the highest revision (7) is neither
                        // the first nor the last row inserted, so a naive "last row wins"
                        // backfill would get this wrong; only MAX(transaction_revision) is correct.
                        .then(seedInboxEvent("event-a", "backfill-multiple-events", 3))
                        .then(seedInboxEvent("event-b", "backfill-multiple-events", 7))
                        .then(seedInboxEvent("event-c", "backfill-multiple-events", 5))
                        .then(seedInboxEvent("event-d", "backfill-single-event", 2)))
                .verifyComplete();

        applyRepositoryMigration("006-replace-payment-batch-impact-inbox.sql");

        StepVerifier.create(Mono.zip(
                        latestAppliedImpactRevision("backfill-multiple-events"),
                        latestAppliedImpactRevision("backfill-single-event"),
                        latestAppliedImpactRevision("backfill-no-events"),
                        rewardBatchImpactInboxTableCount()
                ))
                .assertNext(result -> {
                    assertEquals(7L, result.getT1());
                    assertEquals(2L, result.getT2());
                    assertEquals(0L, result.getT3());
                    assertEquals(0L, result.getT4());
                })
                .verifyComplete();
    }

    private static Mono<Void> seedTransaction(String transactionId) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (transaction_id, initiative_id, accrued_reward_cents)
                        VALUES (:transactionId, 'initiative-1', 0)
                        """)
                .bind("transactionId", transactionId)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> seedInboxEvent(String eventId, String transactionId, long revision) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batch_impact_inbox (
                            event_id, transaction_id, transaction_revision, impact_type
                        )
                        VALUES (:eventId, :transactionId, :revision, 'INVOICE_REPLACED')
                        """)
                .bind("eventId", eventId)
                .bind("transactionId", transactionId)
                .bind("revision", revision)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Long> latestAppliedImpactRevision(String transactionId) {
        return databaseClient()
                .sql("""
                        SELECT latest_applied_payment_impact_revision AS revision
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", transactionId)
                .map((row, metadata) -> row.get("revision", Long.class))
                .one();
    }

    private static Mono<Long> rewardBatchImpactInboxTableCount() {
        return databaseClient()
                .sql("""
                        SELECT COUNT(*) AS count
                        FROM information_schema.tables
                        WHERE table_name = 'reward_batch_impact_inbox'
                        """)
                .map((row, metadata) -> row.get("count", Long.class))
                .one();
    }
}
