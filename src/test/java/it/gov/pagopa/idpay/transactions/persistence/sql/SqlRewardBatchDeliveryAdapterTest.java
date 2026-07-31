package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDate;
import java.time.Month;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchDeliveryAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE = "initiative-delivery";
    private static final String BATCH = "batch-delivery";
    private static SqlRewardBatchDeliveryAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardBatchDeliveryAdapter(
                transactionalOperator(), connectionFactory(), new RewardBatchSqlMapper(JsonMapper.builder().build())
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
    void shouldSnapshotPositiveDerivedAmountOnceAndPersistAcceptedDelivery() {
        DeliveryOutcomeDTO accepted = outcome(true, "accepted");
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH, RewardBatchStatus.APPROVED),
                        insertTransaction("to-check", BATCH, RewardBatchTrxStatus.TO_CHECK, 100),
                        insertTransaction("approved", BATCH, RewardBatchTrxStatus.APPROVED, 200),
                        insertTransaction("rejected", BATCH, RewardBatchTrxStatus.REJECTED, 900)
                ).then(adapter.snapshotDeliveryAmount(BATCH, INITIATIVE))
                        .flatMap(first -> updateTransactionAmount("approved", 500)
                                .then(adapter.snapshotDeliveryAmount(BATCH, INITIATIVE))
                                .flatMap(second -> adapter.recordDeliveryOutcome(BATCH, INITIATIVE, accepted)
                                        .map(delivered -> new DeliveryResult(first.getDeliveryAmountCents(),
                                                second.getDeliveryAmountCents(), delivered))))
                        )
                .assertNext(result -> {
                    assertEquals(300L, result.firstSnapshot());
                    assertEquals(300L, result.secondSnapshot());
                    assertEquals(RewardBatchStatus.PENDING_REFUND, result.delivered().getStatus());
                    assertEquals(300L, result.delivered().getDeliveryAmountCents());
                    assertEquals("accepted", result.delivered().getDeliveryOutcome().getMessage());
                    assertNotNull(result.delivered().getDeliveryDateRequest());
                })
                .verifyComplete();

        StepVerifier.create(adapter.recordDeliveryOutcome(BATCH, INITIATIVE, accepted))
                .assertNext(batch -> assertEquals(RewardBatchStatus.PENDING_REFUND, batch.getStatus()))
                .verifyComplete();
    }

    @Test
    void shouldKeepOneDeliverySnapshotWhenWorkersRace() {
        StepVerifier.create(Flux.concat(
                        insertBatch(BATCH, RewardBatchStatus.APPROVED),
                        insertTransaction("approved", BATCH, RewardBatchTrxStatus.APPROVED, 250)
                ).thenMany(Flux.merge(
                        adapter.snapshotDeliveryAmount(BATCH, INITIATIVE),
                        adapter.snapshotDeliveryAmount(BATCH, INITIATIVE)
                ).collectList())
                        .flatMap(snapshots -> persistedDeliveryAmount(BATCH)
                                .map(persisted -> new ConcurrentSnapshots(snapshots, persisted))))
                .assertNext(result -> {
                    assertEquals(2, result.snapshots().size());
                    assertEquals(250L, result.snapshots().getFirst().getDeliveryAmountCents());
                    assertEquals(250L, result.snapshots().getLast().getDeliveryAmountCents());
                    assertEquals(250L, result.persistedAmount());
                })
                .verifyComplete();
    }

    @Test
    void shouldKeepApprovedOnRejectedDeliveryAndRecordTerminalRefundsIdempotently() {
        DeliveryOutcomeDTO rejected = outcome(false, "rejected");
        StepVerifier.create(insertBatch(BATCH, RewardBatchStatus.APPROVED)
                        .then(setDeliveryAmount(BATCH, 250))
                        .then(adapter.recordDeliveryOutcome(BATCH, INITIATIVE, rejected)))
                .assertNext(batch -> {
                    assertEquals(RewardBatchStatus.APPROVED, batch.getStatus());
                    assertEquals("rejected", batch.getDeliveryOutcome().getMessage());
                })
                .verifyComplete();

        StepVerifier.create(insertBatch("refund", RewardBatchStatus.PENDING_REFUND)
                        .then(adapter.recordRefundOutcome(
                                "refund", INITIATIVE, RewardBatchStatus.REFUNDED, LocalDate.of(2026, Month.JULY, 10), null
                        ))
                        .flatMap(first -> adapter.recordRefundOutcome(
                                "refund", INITIATIVE, RewardBatchStatus.REFUNDED, LocalDate.of(2026, Month.JULY, 11), "ignored"
                        )))
                .assertNext(batch -> {
                    assertEquals(RewardBatchStatus.REFUNDED, batch.getStatus());
                    assertEquals(LocalDate.of(2026, Month.JULY, 10), batch.getRefundValutaDate());
                    assertNotNull(batch.getRefundOutcomeTimestamp());
                })
                .verifyComplete();

        StepVerifier.create(insertBatch("not-refund", RewardBatchStatus.PENDING_REFUND)
                        .then(adapter.recordRefundOutcome(
                                "not-refund", INITIATIVE, RewardBatchStatus.NOT_REFUNDED, null, "external rejection"
                        )))
                .assertNext(batch -> {
                    assertEquals(RewardBatchStatus.NOT_REFUNDED, batch.getStatus());
                    assertEquals("external rejection", batch.getRefundErrorMessage());
                    assertNotNull(batch.getRefundOutcomeTimestamp());
                })
                .verifyComplete();

        StepVerifier.create(adapter.recordRefundOutcome(
                BATCH, INITIATIVE, RewardBatchStatus.REFUNDED, LocalDate.now(), null
        )).verifyComplete();
    }

    private static Mono<Void> insertBatch(String id, RewardBatchStatus status) {
        return databaseClient().sql("""
                        INSERT INTO reward_batches (id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level)
                        VALUES (:id, :initiative, :merchant, '2026-07', 'PHYSICAL', :status, 'July', :assignee)
                        """)
                .bind("id", id).bind("initiative", INITIATIVE).bind("merchant", id).bind("status", status.name())
                .bind("assignee", RewardBatchAssignee.L3.name()).fetch().rowsUpdated().then();
    }

    private static Mono<Void> insertTransaction(String id, String batch, RewardBatchTrxStatus status, long amount) {
        return databaseClient().sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id, reward_batch_trx_status, accrued_reward_cents
                        ) VALUES (:id, :initiative, :batch, :status, :amount)
                        """)
                .bind("id", id).bind("initiative", INITIATIVE).bind("batch", batch)
                .bind("status", status.name()).bind("amount", amount).fetch().rowsUpdated().then();
    }

    private static Mono<Void> updateTransactionAmount(String id, long amount) {
        return databaseClient().sql("UPDATE reward_transactions SET accrued_reward_cents = :amount WHERE transaction_id = :id")
                .bind("id", id).bind("amount", amount).fetch().rowsUpdated().then();
    }

    private static Mono<Void> setDeliveryAmount(String id, long amount) {
        return databaseClient().sql("UPDATE reward_batches SET delivery_amount_cents = :amount WHERE id = :id")
                .bind("id", id).bind("amount", amount).fetch().rowsUpdated().then();
    }

    private static Mono<Long> persistedDeliveryAmount(String id) {
        return databaseClient().sql("SELECT delivery_amount_cents FROM reward_batches WHERE id = :id")
                .bind("id", id)
                .map((row, metadata) -> row.get("delivery_amount_cents", Long.class))
                .one();
    }

    private static DeliveryOutcomeDTO outcome(boolean succeeded, String message) {
        return DeliveryOutcomeDTO.builder().idRichiesta("request").succeded(succeeded).message(message).code(200).build();
    }

    private record DeliveryResult(long firstSnapshot, long secondSnapshot,
                                  it.gov.pagopa.idpay.transactions.model.RewardBatch delivered) {
    }

    private record ConcurrentSnapshots(
            java.util.List<it.gov.pagopa.idpay.transactions.model.RewardBatch> snapshots,
            long persistedAmount
    ) {
    }
}
