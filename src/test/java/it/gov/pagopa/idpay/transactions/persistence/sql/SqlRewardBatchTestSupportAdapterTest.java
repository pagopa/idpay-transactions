package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.YearMonth;
import java.time.format.TextStyle;
import java.util.Locale;
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
class SqlRewardBatchTestSupportAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE = "initiative-test-support";
    private static final String MERCHANT = "merchant-test-support";
    private static final String SOURCE = "source-batch";
    private static SqlRewardBatchTestSupportAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardBatchTestSupportAdapter(
                transactionalOperator(),
                connectionFactory(),
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
    void movesCurrentCreatedBatchAndPreservesMembershipAndLifecycleMetadata() {
        YearMonth current = YearMonth.now(ZONEID);
        YearMonth expected = current.minusMonths(1);
        LocalDateTime originalUpdate = LocalDateTime.of(2025, Month.JANUARY, 2, 3, 4, 5);
        LocalDateTime creation = LocalDateTime.of(2024, Month.FEBRUARY, 3, 4, 5, 6);
        LocalDateTime merchantSend = LocalDateTime.of(2025, Month.MARCH, 4, 5, 6, 7);

        StepVerifier.create(insertDetailedBatch(
                        SOURCE, INITIATIVE, MERCHANT, current, RewardBatchStatus.CREATED,
                        originalUpdate, creation, merchantSend
                )
                .then(insertTransaction("member", INITIATIVE, SOURCE))
                .then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .assertNext(prepared -> {
                    assertEquals(SOURCE, prepared.rewardBatchId());
                    assertEquals(current.toString(), prepared.previousMonth());
                    assertEquals(expected.toString(), prepared.referenceMonth());
                    assertNotEquals(originalUpdate, prepared.updateDate());
                })
                .verifyComplete();

        StepVerifier.create(Mono.zip(batchSnapshot(SOURCE), membership("member")))
                .assertNext(result -> {
                    BatchSnapshot batch = result.getT1();
                    assertEquals(SOURCE, batch.id());
                    assertEquals(INITIATIVE, batch.initiative());
                    assertEquals(MERCHANT, batch.merchant());
                    assertEquals("PHYSICAL", batch.posType());
                    assertEquals("Business Name", batch.businessName());
                    assertEquals(RewardBatchStatus.CREATED.name(), batch.status());
                    assertEquals(expected.toString(), batch.month());
                    assertEquals(expected.atDay(1).atStartOfDay(), batch.startDate());
                    assertEquals(expected.atEndOfMonth().atTime(23, 59, 59), batch.endDate());
                    assertEquals(expected.getMonth().getDisplayName(TextStyle.FULL, Locale.ITALIAN)
                            + " " + expected.getYear(), batch.name());
                    assertEquals(creation, batch.creationDate());
                    assertEquals(merchantSend, batch.merchantSendDate());
                    assertEquals("report/path", batch.reportPath());
                    assertEquals("report.csv", batch.filename());
                    assertTrue(batch.updateDate().isAfter(originalUpdate));
                    assertEquals(SOURCE, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void skipsOccupiedPreviousGroupingAndLeavesItUnchanged() {
        YearMonth current = YearMonth.now(ZONEID);
        LocalDateTime occupiedUpdate = LocalDateTime.of(2025, Month.MAY, 6, 7, 8);

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE, INITIATIVE, MERCHANT, current, RewardBatchStatus.CREATED),
                        insertDetailedBatch("occupied", INITIATIVE, MERCHANT, current.minusMonths(1),
                                RewardBatchStatus.SENT, occupiedUpdate,
                                LocalDateTime.of(2025, Month.JANUARY, 1, 0, 0), null)
                ).then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .assertNext(prepared -> assertEquals(
                        current.minusMonths(2).toString(), prepared.referenceMonth()
                ))
                .verifyComplete();

        StepVerifier.create(batchSnapshot("occupied"))
                .assertNext(batch -> {
                    assertEquals(current.minusMonths(1).toString(), batch.month());
                    assertEquals(RewardBatchStatus.SENT.name(), batch.status());
                    assertEquals(occupiedUpdate, batch.updateDate());
                })
                .verifyComplete();
    }

    @Test
    void selectsBeforeEarlierNonEmptyCreatedBatchButIgnoresAnEmptyOne() {
        YearMonth current = YearMonth.now(ZONEID);

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE, INITIATIVE, MERCHANT, current, RewardBatchStatus.CREATED),
                        insertBatch("blocker", INITIATIVE, MERCHANT, current.minusMonths(3),
                                RewardBatchStatus.CREATED),
                        insertTransaction("blocking-member", INITIATIVE, "blocker")
                ).then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .assertNext(prepared -> assertEquals(
                        current.minusMonths(4).toString(), prepared.referenceMonth()
                ))
                .verifyComplete();

        clearDatabase();

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE, INITIATIVE, MERCHANT, current, RewardBatchStatus.CREATED),
                        insertBatch("empty", INITIATIVE, MERCHANT, current.minusMonths(3),
                                RewardBatchStatus.CREATED)
                ).then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .assertNext(prepared -> assertEquals(
                        current.minusMonths(1).toString(), prepared.referenceMonth()
                ))
                .verifyComplete();
    }

    @Test
    void alreadyPastSafeBatchIsIdempotentAndKeepsUpdateDate() {
        YearMonth past = YearMonth.now(ZONEID).minusMonths(2);
        LocalDateTime originalUpdate = LocalDateTime.of(2025, Month.JUNE, 7, 8, 9, 10);

        StepVerifier.create(insertDetailedBatch(
                        SOURCE, INITIATIVE, MERCHANT, past, RewardBatchStatus.CREATED,
                        originalUpdate,
                        LocalDateTime.of(2025, Month.JANUARY, 1, 0, 0),
                        null
                )
                .then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .assertNext(prepared -> {
                    assertEquals(past.toString(), prepared.previousMonth());
                    assertEquals(past.toString(), prepared.referenceMonth());
                    assertEquals(originalUpdate, prepared.updateDate());
                })
                .verifyComplete();

        StepVerifier.create(batchSnapshot(SOURCE))
                .assertNext(batch -> assertEquals(originalUpdate, batch.updateDate()))
                .verifyComplete();
    }

    @Test
    void pastBatchMovesBeforeAnEarlierNonEmptyCreatedBatch() {
        YearMonth current = YearMonth.now(ZONEID);
        YearMonth sourceMonth = current.minusMonths(2);

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE, INITIATIVE, MERCHANT, sourceMonth,
                                RewardBatchStatus.CREATED),
                        insertBatch("blocker", INITIATIVE, MERCHANT, current.minusMonths(3),
                                RewardBatchStatus.CREATED),
                        insertTransaction("blocking-member", INITIATIVE, "blocker")
                ).then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .assertNext(prepared -> assertEquals(
                        current.minusMonths(4).toString(),
                        prepared.referenceMonth()
                ))
                .verifyComplete();
    }

    @Test
    void earlierNonEmptySentBatchDoesNotBlockTheNearestAvailableMonth() {
        YearMonth current = YearMonth.now(ZONEID);

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE, INITIATIVE, MERCHANT, current,
                                RewardBatchStatus.CREATED),
                        insertBatch("sent", INITIATIVE, MERCHANT, current.minusMonths(3),
                                RewardBatchStatus.SENT),
                        insertTransaction("sent-member", INITIATIVE, "sent")
                ).then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .assertNext(prepared -> assertEquals(
                        current.minusMonths(1).toString(),
                        prepared.referenceMonth()
                ))
                .verifyComplete();
    }

    @Test
    void rejectsMissingOrMismatchedInitiativeWithoutChangingBatch() {
        YearMonth current = YearMonth.now(ZONEID);

        StepVerifier.create(insertBatch(SOURCE, INITIATIVE, MERCHANT, current, RewardBatchStatus.CREATED)
                .then(adapter.prepareForSend("other-initiative", SOURCE, 12)))
                .expectErrorSatisfies(error -> assertClientError(
                        error, 404, ExceptionCode.REWARD_BATCH_NOT_FOUND
                ))
                .verify();

        StepVerifier.create(adapter.prepareForSend(INITIATIVE, "missing", 12))
                .expectErrorSatisfies(error -> assertClientError(
                        error, 404, ExceptionCode.REWARD_BATCH_NOT_FOUND
                ))
                .verify();

        StepVerifier.create(batchSnapshot(SOURCE))
                .assertNext(batch -> assertEquals(current.toString(), batch.month()))
                .verifyComplete();
    }

    @Test
    void nonCreatedSourceIsRejectedAndCasPredicateLeavesItUnchanged() {
        YearMonth current = YearMonth.now(ZONEID);
        LocalDateTime originalUpdate = LocalDateTime.of(2025, Month.JULY, 8, 9, 10);

        StepVerifier.create(insertDetailedBatch(
                        SOURCE, INITIATIVE, MERCHANT, current, RewardBatchStatus.SENT,
                        originalUpdate,
                        LocalDateTime.of(2025, Month.JANUARY, 1, 0, 0),
                        null
                )
                .then(adapter.prepareForSend(INITIATIVE, SOURCE, 12)))
                .expectErrorSatisfies(error -> assertClientError(
                        error, 409, ExceptionCode.REWARD_BATCH_STATUS_NOT_ALLOWED
                ))
                .verify();

        StepVerifier.create(batchSnapshot(SOURCE))
                .assertNext(batch -> {
                    assertEquals(current.toString(), batch.month());
                    assertEquals(RewardBatchStatus.SENT.name(), batch.status());
                    assertEquals(originalUpdate, batch.updateDate());
                })
                .verifyComplete();
    }

    @Test
    void reportsHorizonExhaustionWithoutChangingSource() {
        YearMonth current = YearMonth.now(ZONEID);

        StepVerifier.create(Flux.concat(
                        insertBatch(SOURCE, INITIATIVE, MERCHANT, current, RewardBatchStatus.CREATED),
                        insertBatch("occupied-1", INITIATIVE, MERCHANT, current.minusMonths(1),
                                RewardBatchStatus.SENT),
                        insertBatch("occupied-2", INITIATIVE, MERCHANT, current.minusMonths(2),
                                RewardBatchStatus.SENT)
                ).then(adapter.prepareForSend(INITIATIVE, SOURCE, 2)))
                .expectErrorSatisfies(error -> assertClientError(
                        error, 409, ExceptionCode.REWARD_BATCH_TEST_SUPPORT_NO_SAFE_MONTH
                ))
                .verify();

        StepVerifier.create(batchSnapshot(SOURCE))
                .assertNext(batch -> assertEquals(current.toString(), batch.month()))
                .verifyComplete();
    }

    @Test
    void rejectsNonPositiveSearchHorizon() {
        StepVerifier.create(adapter.prepareForSend(INITIATIVE, SOURCE, 0))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Reward batch search horizon must be positive".equals(error.getMessage()))
                .verify();
    }

    private static Mono<Void> insertBatch(
            String id,
            String initiative,
            String merchant,
            YearMonth month,
            RewardBatchStatus status
    ) {
        return insertDetailedBatch(
                id, initiative, merchant, month, status,
                LocalDateTime.of(2025, Month.JANUARY, 2, 3, 4),
                LocalDateTime.of(2025, Month.JANUARY, 1, 2, 3),
                null
        );
    }

    private static Mono<Void> insertDetailedBatch(
            String id,
            String initiative,
            String merchant,
            YearMonth month,
            RewardBatchStatus status,
            LocalDateTime updateDate,
            LocalDateTime creationDate,
            LocalDateTime merchantSendDate
    ) {
        var spec = databaseClient().sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, business_name, month, pos_type,
                            status, name, start_date, end_date, creation_date, update_date,
                            merchant_send_date, report_path, filename, assignee_level
                        ) VALUES (
                            :id, :initiative, :merchant, 'Business Name', :month, 'PHYSICAL',
                            :status, 'Original Name', :startDate, :endDate, :creationDate,
                            :updateDate, :merchantSendDate, 'report/path', 'report.csv', 'L1'
                        )
                        """)
                .bind("id", id)
                .bind("initiative", initiative)
                .bind("merchant", merchant)
                .bind("month", month.toString())
                .bind("status", status.name())
                .bind("startDate", month.atDay(1).atStartOfDay())
                .bind("endDate", month.atEndOfMonth().atTime(23, 59, 59))
                .bind("creationDate", creationDate)
                .bind("updateDate", updateDate);
        spec = merchantSendDate == null
                ? spec.bindNull("merchantSendDate", LocalDateTime.class)
                : spec.bind("merchantSendDate", merchantSendDate);
        return spec.fetch().rowsUpdated().then();
    }

    private static Mono<Void> insertTransaction(String id, String initiative, String batchId) {
        return databaseClient().sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id,
                            reward_batch_trx_status, accrued_reward_cents
                        ) VALUES (:id, :initiative, :batchId, 'CONSULTABLE', 0)
                        """)
                .bind("id", id)
                .bind("initiative", initiative)
                .bind("batchId", batchId)
                .fetch().rowsUpdated().then();
    }

    private static Mono<BatchSnapshot> batchSnapshot(String id) {
        return databaseClient().sql("""
                        SELECT id, initiative_id, merchant_id, pos_type, business_name, status,
                               month, start_date, end_date, name, creation_date, update_date,
                               merchant_send_date, report_path, filename
                        FROM reward_batches WHERE id = :id
                        """)
                .bind("id", id)
                .map((row, metadata) -> new BatchSnapshot(
                        row.get("id", String.class),
                        row.get("initiative_id", String.class),
                        row.get("merchant_id", String.class),
                        row.get("pos_type", String.class),
                        row.get("business_name", String.class),
                        row.get("status", String.class),
                        row.get("month", String.class),
                        row.get("start_date", LocalDateTime.class),
                        row.get("end_date", LocalDateTime.class),
                        row.get("name", String.class),
                        row.get("creation_date", LocalDateTime.class),
                        row.get("update_date", LocalDateTime.class),
                        row.get("merchant_send_date", LocalDateTime.class),
                        row.get("report_path", String.class),
                        row.get("filename", String.class)
                ))
                .one();
    }

    private static Mono<String> membership(String transactionId) {
        return databaseClient().sql("""
                        SELECT reward_batch_id FROM reward_transactions WHERE transaction_id = :id
                        """)
                .bind("id", transactionId)
                .map((row, metadata) -> row.get("reward_batch_id", String.class))
                .one();
    }

    private static void assertClientError(Throwable error, int status, String code) {
        assertTrue(error instanceof ClientExceptionWithBody);
        ClientExceptionWithBody exception = (ClientExceptionWithBody) error;
        assertEquals(status, exception.getHttpStatus().value());
        assertEquals(code, exception.getCode());
    }

    private record BatchSnapshot(
            String id,
            String initiative,
            String merchant,
            String posType,
            String businessName,
            String status,
            String month,
            LocalDateTime startDate,
            LocalDateTime endDate,
            String name,
            LocalDateTime creationDate,
            LocalDateTime updateDate,
            LocalDateTime merchantSendDate,
            String reportPath,
            String filename
    ) {
    }
}
