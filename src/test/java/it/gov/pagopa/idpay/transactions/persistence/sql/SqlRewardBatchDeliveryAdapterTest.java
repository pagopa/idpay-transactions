package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.InvitaliaOutcomeStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import org.jooq.JSONB;
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
        databaseClient().sql("DELETE FROM reward_batch_delivery_events").fetch().rowsUpdated()
                .then(databaseClient().sql("DELETE FROM reward_batch_delivery_transactions").fetch().rowsUpdated())
                .then(databaseClient().sql("DELETE FROM reward_transactions").fetch().rowsUpdated())
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
    void shouldPersistEligibleTransactionsSnapshotOnceAndNotRewriteAfterChanges() {
        StepVerifier.create(Flux.concat(
                                insertBatch(BATCH, RewardBatchStatus.APPROVED),
                                insertTransaction("t1", BATCH, RewardBatchTrxStatus.TO_CHECK, 100),
                                insertTransaction("t2", BATCH, RewardBatchTrxStatus.CONSULTABLE, 150),
                                insertTransaction("t3", BATCH, RewardBatchTrxStatus.APPROVED, 250),
                                insertTransaction("t4", BATCH, RewardBatchTrxStatus.REJECTED, 999)
                        ).then(adapter.snapshotDeliveryAmount(BATCH, INITIATIVE))
                        .then(updateTransactionAmount("t1", 700))
                        .then(markTransactionRefunded("t2"))
                        .then(adapter.snapshotDeliveryAmount(BATCH, INITIATIVE))
                        .then(snapshotRowCount(BATCH).zipWith(snapshotAmountSum(BATCH))))
                .assertNext(tuple -> {
                    assertEquals(3L, tuple.getT1());
                    assertEquals(500L, tuple.getT2());
                })
                .verifyComplete();

        StepVerifier.create(snapshotTransactions(BATCH).collectList())
                .assertNext(rows -> assertEquals(List.of("t1:100:TO_CHECK", "t2:150:CONSULTABLE", "t3:250:APPROVED"), rows))
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
                                .zipWith(snapshotRowCount(BATCH))
                                .map(persisted -> new ConcurrentSnapshots(snapshots, persisted.getT1(), persisted.getT2()))))
                .assertNext(result -> {
                    assertEquals(2, result.snapshots().size());
                    assertEquals(250L, result.snapshots().getFirst().getDeliveryAmountCents());
                    assertEquals(250L, result.snapshots().getLast().getDeliveryAmountCents());
                    assertEquals(250L, result.persistedAmount());
                    assertEquals(1L, result.snapshotRows());
                })
                .verifyComplete();
    }

    @Test
    void shouldRecordDeliveryEventsWithIncrementalAttemptNumberAndWithoutDuplicatesOnRace() {
        JSONB reqJson = JSONB.jsonb("{\"id\":\"batch-delivery\",\"erogazione\":{\"ibanBeneficiario\":\"****1234\"}}");
        JSONB resJson = JSONB.jsonb("{\"esito\":\"OK\"}");

        StepVerifier.create(insertBatch(BATCH, RewardBatchStatus.APPROVED)
                        .then(adapter.recordDeliveryEvent(BATCH, INITIATIVE, "REJECTED", 400, reqJson, resJson))
                        .then(adapter.recordDeliveryEvent(BATCH, INITIATIVE, "TECHNICAL_ERROR", null, reqJson, null))
                        .then(adapter.recordDeliveryEvent(BATCH, INITIATIVE, "ACCEPTED", 200, reqJson, resJson))
                        .thenMany(Flux.merge(
                                adapter.recordDeliveryEvent(BATCH, INITIATIVE, "ACCEPTED", 200, reqJson, resJson),
                                adapter.recordDeliveryEvent(BATCH, INITIATIVE, "ACCEPTED", 200, reqJson, resJson)
                        ))
                        .then(deliveryEventsSummary(BATCH).collectList()))
                .assertNext(events -> {
                    assertTrue(events.size() >= 3);
                    assertEquals("1:REJECTED:400", events.get(0));
                    assertEquals("2:TECHNICAL_ERROR:null", events.get(1));
                    assertEquals("3:ACCEPTED:200", events.get(2));
                })
                .verifyComplete();
    }

    @Test
    void shouldUpdateOutcomeColumnsOnlyRecordOutcomeEventOnStatusChangeAndHandleExistingPendingRefundBatch() {
        JSONB firstResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"IN_LAVORAZIONE\"}}");
        JSONB secondResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"IN_LAVORAZIONE\"}}");
        JSONB completedResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"COMPLETATA\",\"amountPaid\":250.00}}");

        OutcomeCheckCommand inLavorazione = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.IN_LAVORAZIONE.name(), null, null, null, null
        );
        OutcomeCheckCommand completata = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.COMPLETATA.name(), 250L, LocalDate.of(2026, Month.JULY, 15), null,
                RewardBatchStatus.REFUNDED
        );

        StepVerifier.create(insertBatch(BATCH, RewardBatchStatus.PENDING_REFUND)
                        .then(setDeliveryAmount(BATCH, 300))
                        .then(adapter.recordOutcomeCheck(BATCH, INITIATIVE, inLavorazione, firstResp))
                        .flatMap(afterFirst -> adapter.recordOutcomeCheck(BATCH, INITIATIVE, inLavorazione, secondResp))
                        .flatMap(afterDuplicate -> adapter.recordOutcomeCheck(BATCH, INITIATIVE, completata, completedResp))
                        .flatMap(afterCompleted -> persistedOutcomeCheckColumns(BATCH)
                                .zipWith(outcomeEventsSummary(BATCH).collectList())
                                .map(tuple -> new OutcomeCheckTestResult(afterCompleted, tuple.getT1(), tuple.getT2()))))
                .assertNext(res -> {
                    assertEquals(RewardBatchStatus.REFUNDED, res.batch().getStatus());
                    assertEquals(InvitaliaOutcomeStatus.COMPLETATA.name(), res.columns().lastInvitaliaStatus());
                    assertNotNull(res.columns().lastOutcomeCheckDate());
                    assertEquals(250L, res.columns().refundAmountPaidCents());
                    assertEquals(List.of("1:IN_LAVORAZIONE:null", "2:COMPLETATA:250"), res.events());
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

    @Test
    void shouldRejectInvalidDeliveryInputsAndPreserveUnprocessableBatchState() {
        StepVerifier.create(adapter.recordDeliveryOutcome(BATCH, INITIATIVE, null))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Delivery outcome is required".equals(error.getMessage()))
                .verify();
        StepVerifier.create(adapter.snapshotDeliveryAmount(" ", INITIATIVE))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch and initiative identifiers are required".equals(error.getMessage()))
                .verify();

        IllegalArgumentException refundStatusError = assertThrows(
                IllegalArgumentException.class,
                () -> adapter.recordRefundOutcome(BATCH, INITIATIVE, RewardBatchStatus.PENDING_REFUND, null, null)
        );
        assertEquals("Unsupported refund outcome status", refundStatusError.getMessage());

        StepVerifier.create(adapter.recordRefundOutcome(BATCH, " ", RewardBatchStatus.REFUNDED, null, null))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch and initiative identifiers are required".equals(error.getMessage()))
                .verify();
        StepVerifier.create(adapter.recordRefundOutcome(BATCH, null, RewardBatchStatus.NOT_REFUNDED, null, null))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch and initiative identifiers are required".equals(error.getMessage()))
                .verify();

        StepVerifier.create(insertBatch("zero", RewardBatchStatus.APPROVED)
                .then(adapter.snapshotDeliveryAmount("zero", INITIATIVE)))
                .verifyComplete();
        StepVerifier.create(deliveryAmountIsNull("zero"))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(insertBatch("missing-snapshot", RewardBatchStatus.APPROVED)
                .then(adapter.recordDeliveryOutcome(
                        "missing-snapshot", INITIATIVE, outcome(true, "accepted")
                )))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("not snapshotted"))
                .verify();
        StepVerifier.create(batchStatus("missing-snapshot"))
                .expectNext(RewardBatchStatus.APPROVED.name())
                .verifyComplete();

        StepVerifier.create(insertBatch("sent", RewardBatchStatus.SENT)
                .then(adapter.recordDeliveryOutcome("sent", INITIATIVE, outcome(false, "rejected"))))
                .verifyComplete();
        StepVerifier.create(batchStatus("sent"))
                .expectNext(RewardBatchStatus.SENT.name())
                .verifyComplete();
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

    private static Mono<Void> markTransactionRefunded(String id) {
        return databaseClient().sql("UPDATE reward_transactions SET status = 'REFUNDED' WHERE transaction_id = :id")
                .bind("id", id).fetch().rowsUpdated().then();
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

    private static Mono<OutcomeCheckColumns> persistedOutcomeCheckColumns(String batchId) {
        return databaseClient().sql("""
                        SELECT last_outcome_check_date, last_invitalia_status, refund_amount_paid_cents
                        FROM reward_batches
                        WHERE id = :batchId
                        """)
                .bind("batchId", batchId)
                .map((row, metadata) -> new OutcomeCheckColumns(
                        row.get("last_outcome_check_date", LocalDateTime.class),
                        row.get("last_invitalia_status", String.class),
                        row.get("refund_amount_paid_cents", Long.class)
                ))
                .one();
    }

    private static Mono<Long> snapshotRowCount(String batchId) {
        return databaseClient().sql("SELECT COUNT(*) AS cnt FROM reward_batch_delivery_transactions WHERE reward_batch_id = :batchId")
                .bind("batchId", batchId)
                .map((row, metadata) -> row.get("cnt", Long.class))
                .one();
    }

    private static Mono<Long> snapshotAmountSum(String batchId) {
        return databaseClient().sql("SELECT COALESCE(SUM(accrued_reward_cents), 0) AS total FROM reward_batch_delivery_transactions WHERE reward_batch_id = :batchId")
                .bind("batchId", batchId)
                .map((row, metadata) -> row.get("total", Long.class))
                .one();
    }

    private static Flux<String> snapshotTransactions(String batchId) {
        return databaseClient().sql("""
                        SELECT transaction_id, accrued_reward_cents, reward_batch_trx_status
                        FROM reward_batch_delivery_transactions
                        WHERE reward_batch_id = :batchId
                        ORDER BY transaction_id
                        """)
                .bind("batchId", batchId)
                .map((row, metadata) -> row.get("transaction_id", String.class)
                        + ":" + row.get("accrued_reward_cents", Long.class)
                        + ":" + row.get("reward_batch_trx_status", String.class))
                .all();
    }

    private static Flux<String> deliveryEventsSummary(String batchId) {
        return databaseClient().sql("""
                        SELECT attempt_number, outcome, http_code
                        FROM reward_batch_delivery_events
                        WHERE reward_batch_id = :batchId AND event_type = 'DELIVERY'
                        ORDER BY attempt_number
                        """)
                .bind("batchId", batchId)
                .map((row, metadata) -> row.get("attempt_number", Integer.class)
                        + ":" + row.get("outcome", String.class)
                        + ":" + row.get("http_code", Integer.class))
                .all();
    }

    private static Flux<String> outcomeEventsSummary(String batchId) {
        return databaseClient().sql("""
                        SELECT attempt_number, invitalia_status, amount_paid_cents
                        FROM reward_batch_delivery_events
                        WHERE reward_batch_id = :batchId AND event_type = 'OUTCOME'
                        ORDER BY attempt_number
                        """)
                .bind("batchId", batchId)
                .map((row, metadata) -> row.get("attempt_number", Integer.class)
                        + ":" + row.get("invitalia_status", String.class)
                        + ":" + row.get("amount_paid_cents", Long.class))
                .all();
    }

    private static Mono<Boolean> deliveryAmountIsNull(String id) {
        return databaseClient().sql("SELECT delivery_amount_cents IS NULL AS amount_is_null FROM reward_batches WHERE id = :id")
                .bind("id", id)
                .map((row, metadata) -> row.get("amount_is_null", Boolean.class))
                .one();
    }

    private static Mono<String> batchStatus(String id) {
        return databaseClient().sql("SELECT status FROM reward_batches WHERE id = :id")
                .bind("id", id)
                .map((row, metadata) -> row.get("status", String.class))
                .one();
    }

    private static DeliveryOutcomeDTO outcome(boolean succeeded, String message) {
        return DeliveryOutcomeDTO.builder().idRichiesta("request").succeded(succeeded).message(message).code(200).build();
    }

    @Test
    void shouldRejectNullOutcomeCheckCommand() {
        StepVerifier.create(adapter.recordOutcomeCheck(BATCH, INITIATIVE, null, null))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Outcome check command is required".equals(error.getMessage()))
                .verify();
    }

    @Test
    void shouldHandleOutcomeCheckWhenBatchIsAlreadyRefundedOrInNonPendingRefundState() {
        JSONB resp = JSONB.jsonb("{\"erogazione\":{\"status\":\"COMPLETATA\"}}");
        OutcomeCheckCommand completataWithAmount = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.COMPLETATA.name(), 999L, LocalDate.of(2026, Month.JULY, 20), null,
                RewardBatchStatus.REFUNDED
        );

        StepVerifier.create(insertBatch("already-refunded", RewardBatchStatus.REFUNDED)
                        .then(setDeliveryAmount("already-refunded", 500))
                        .then(adapter.recordOutcomeCheck("already-refunded", INITIATIVE, completataWithAmount, resp))
                        .then(persistedOutcomeCheckColumns("already-refunded")))
                .assertNext(cols -> {
                    assertEquals(InvitaliaOutcomeStatus.COMPLETATA.name(), cols.lastInvitaliaStatus());
                    assertNotNull(cols.lastOutcomeCheckDate());
                    assertEquals(null, cols.refundAmountPaidCents());
                })
                .verifyComplete();

        // Lotto APPROVED con targetStatusIfAny valorizzato: stato non PENDING_REFUND -> Mono.empty()
        StepVerifier.create(insertBatch("approved-batch", RewardBatchStatus.APPROVED)
                        .then(adapter.recordOutcomeCheck("approved-batch", INITIATIVE, completataWithAmount, resp)))
                .verifyComplete();
    }

    @Test
    void shouldNotFailOutcomeCheckWhenOutcomeEventInsertErrors() {
        JSONB validResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"COMPLETATA\"}}");
        OutcomeCheckCommand completata = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.COMPLETATA.name(), 200L, LocalDate.of(2026, Month.JULY, 15), null,
                RewardBatchStatus.REFUNDED
        );

        StepVerifier.create(insertBatch("event-err", RewardBatchStatus.PENDING_REFUND)
                        .then(setDeliveryAmount("event-err", 300))
                        .then(insertOutcomeEvent("event-err", 1, "PREVIOUS", 100L))
                        .then(insertOutcomeEvent("event-err", 2, "OTHER", 100L))
                        .then(adapter.recordOutcomeCheck("event-err", INITIATIVE, completata, validResp)))
                .assertNext(batch -> assertEquals(RewardBatchStatus.REFUNDED, batch.getStatus()))
                .verifyComplete();
    }

    private static Mono<Void> insertOutcomeEvent(String batchId, int attemptNumber, String status, Long amountPaid) {
        return databaseClient().sql("""
                    INSERT INTO reward_batch_delivery_events (
                        reward_batch_id, initiative_id, event_type, attempt_number,
                        invitalia_status, amount_paid_cents, created_at
                    ) VALUES (:batchId, :initiative, 'OUTCOME', :attempt, :status, :amount, CURRENT_TIMESTAMP)
                    """)
                .bind("batchId", batchId)
                .bind("initiative", INITIATIVE)
                .bind("attempt", attemptNumber)
                .bind("status", status)
                .bind("amount", amountPaid)
                .fetch().rowsUpdated().then();
    }

    @Test
    void shouldSwallowOutcomeEventErrorAndStillReturnUpdatedBatch() {
        SqlRewardBatchDeliveryAdapter spiedAdapter = spy(adapter);
        JSONB respJson = JSONB.jsonb("{\"erogazione\":{\"status\":\"COMPLETATA\",\"amountPaid\":250.00}}");
        OutcomeCheckCommand completata = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.COMPLETATA.name(), 250L, LocalDate.of(2026, Month.JULY, 15), null,
                RewardBatchStatus.REFUNDED
        );

        doReturn(Mono.error(new RuntimeException("failed to write outcome event")))
                .when(spiedAdapter)
                .recordOutcomeEvent(eq("event-fail"), eq(INITIATIVE), eq("COMPLETATA"), eq(250L), eq(respJson));

        StepVerifier.create(insertBatch("event-fail", RewardBatchStatus.PENDING_REFUND)
                        .then(setDeliveryAmount("event-fail", 250))
                        .then(spiedAdapter.recordOutcomeCheck("event-fail", INITIATIVE, completata, respJson)))
                .assertNext(batch -> assertEquals(RewardBatchStatus.REFUNDED, batch.getStatus()))
                .verifyComplete();
    }

    @Test
    void shouldLogMismatchOnCompletataWithDifferentAmountAndSkipPaidAmountWhenNotCompletata() {
        JSONB processingResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"IN_LAVORAZIONE\"}}");
        JSONB mismatchResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"COMPLETATA\",\"amountPaid\":250.00}}");

        OutcomeCheckCommand inLavorazione = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.IN_LAVORAZIONE.name(), null, null, null, null
        );
        OutcomeCheckCommand completataMismatch = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.COMPLETATA.name(), 250L, LocalDate.of(2026, Month.JULY, 16), null,
                RewardBatchStatus.REFUNDED
        );

        StepVerifier.create(insertBatch("mismatch-batch", RewardBatchStatus.PENDING_REFUND)
                        .then(setDeliveryAmount("mismatch-batch", 300))
                        .then(adapter.recordOutcomeCheck("mismatch-batch", INITIATIVE, inLavorazione, processingResp))
                        .then(adapter.recordOutcomeCheck("mismatch-batch", INITIATIVE, completataMismatch, mismatchResp))
                        .then(persistedOutcomeCheckColumns("mismatch-batch")))
                .assertNext(cols -> {
                    assertEquals(InvitaliaOutcomeStatus.COMPLETATA.name(), cols.lastInvitaliaStatus());
                    assertEquals(250L, cols.refundAmountPaidCents());
                    assertNotNull(cols.lastOutcomeCheckDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldCoverCompletataWithNullAmountAndNonCompletataStatus() {
        JSONB rejectedResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"RIFIUTATA\"}}");
        JSONB completedNullAmountResp = JSONB.jsonb("{\"erogazione\":{\"status\":\"COMPLETATA\"}}");

        OutcomeCheckCommand rifiutata = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.RIFIUTATA.name(), null, null, "rejected",
                RewardBatchStatus.NOT_REFUNDED
        );
        OutcomeCheckCommand completataSenzaImporto = new OutcomeCheckCommand(
                InvitaliaOutcomeStatus.COMPLETATA.name(), null, LocalDate.of(2026, Month.JULY, 18), null,
                RewardBatchStatus.REFUNDED
        );

        StepVerifier.create(insertBatch("non-completata-batch", RewardBatchStatus.PENDING_REFUND)
                        .then(setDeliveryAmount("non-completata-batch", 200))
                        .then(adapter.recordOutcomeCheck("non-completata-batch", INITIATIVE, rifiutata, rejectedResp))
                        .then(persistedOutcomeCheckColumns("non-completata-batch")))
                .assertNext(cols -> {
                    assertEquals(InvitaliaOutcomeStatus.RIFIUTATA.name(), cols.lastInvitaliaStatus());
                    assertNull(cols.refundAmountPaidCents());
                })
                .verifyComplete();

        StepVerifier.create(insertBatch("completata-null-amount", RewardBatchStatus.PENDING_REFUND)
                        .then(setDeliveryAmount("completata-null-amount", 200))
                        .then(adapter.recordOutcomeCheck("completata-null-amount", INITIATIVE, completataSenzaImporto, completedNullAmountResp))
                        .then(persistedOutcomeCheckColumns("completata-null-amount")))
                .assertNext(cols -> {
                    assertEquals(InvitaliaOutcomeStatus.COMPLETATA.name(), cols.lastInvitaliaStatus());
                    assertNull(cols.refundAmountPaidCents());
                })
                .verifyComplete();
    }

    private record DeliveryResult(long firstSnapshot, long secondSnapshot, RewardBatch delivered) {
    }

    private record ConcurrentSnapshots(List<RewardBatch> snapshots, Long persistedAmount, Long snapshotRows) {
    }

    private record OutcomeCheckColumns(
            LocalDateTime lastOutcomeCheckDate,
            String lastInvitaliaStatus,
            Long refundAmountPaidCents
    ) {
    }

    private record OutcomeCheckTestResult(RewardBatch batch, OutcomeCheckColumns columns, List<String> events) {
    }
}
