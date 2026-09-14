package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.http.HttpStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.YearMonth;
import java.util.List;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_MONTH_TOO_EARLY;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_PREVIOUS_NOT_SENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlRewardBatchAdapter adapter;
    private static SqlRewardBatchListAdapter listAdapter;
    private static SqlRewardBatchEvaluationAdapter evaluationAdapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardBatchAdapter(
                transactionalOperator(),
                DSL.using(
                        new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                        SQLDialect.POSTGRES
                ),
                connectionFactory(),
                new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                        .getRepository(RewardBatchSqlRepository.class),
                new RewardBatchSqlMapper(JsonMapper.builder().build())
        );
        listAdapter = new SqlRewardBatchListAdapter(
                DSL.using(
                        new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                        SQLDialect.POSTGRES
                ),
                new RewardBatchSqlMapper(JsonMapper.builder().build())
        );
        evaluationAdapter = new SqlRewardBatchEvaluationAdapter(
                transactionalOperator(),
                connectionFactory(),
                new RewardBatchSqlMapper(JsonMapper.builder().build()),
                new RewardTransactionSqlMapper(JsonMapper.builder().build())
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
    void shouldCreateOrReadExactlyOneBatchWhenGroupingInsertsRace() {
        RewardBatch first = batch("batch-first");
        RewardBatch second = batch("batch-second");

        StepVerifier.create(Flux.merge(
                        adapter.createOrRead(first),
                        adapter.createOrRead(second)
                ).collectList())
                .assertNext(created -> {
                    assertEquals(2, created.size());
                    assertTrue(created.stream()
                            .map(RewardBatch::getId)
                            .allMatch(created.getFirst().getId()::equals));
                })
                .verifyComplete();

        StepVerifier.create(adapter.findByGrouping(
                        first.getInitiativeId(),
                        first.getMerchantId(),
                        first.getPosType(),
                        first.getMonth()
                ))
                .expectNextMatches(found -> found.getId().equals(first.getId()) || found.getId().equals(second.getId()))
                .verifyComplete();
    }

    @Test
    void shouldReadBatchOnlyWithinItsRequestedIdentityScope() {
        RewardBatch batch = batch("batch-identity");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(adapter.findByIdAndInitiativeId(batch.getId(), batch.getInitiativeId())))
                .expectNextMatches(found -> found.getId().equals(batch.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findByMerchantInitiativeAndId(
                        batch.getMerchantId(),
                        batch.getInitiativeId(),
                        batch.getId()
                ))
                .expectNextMatches(found -> found.getId().equals(batch.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findByIdAndInitiativeId(batch.getId(), "other-initiative"))
                .verifyComplete();

        StepVerifier.create(adapter.findByMerchantInitiativeAndId(
                        "other-merchant",
                        batch.getInitiativeId(),
                        batch.getId()
                ))
                .verifyComplete();
    }

    @Test
    void shouldPersistOrdinaryCrudThroughTheR2dbcRepository() {
        RewardBatch batch = batch("batch-crud");

        StepVerifier.create(adapter.save(batch)
                        .flatMap(saved -> {
                            saved.setBusinessName("Updated merchant");
                            return adapter.save(saved);
                        })
                        .flatMap(saved -> adapter.findById(saved.getId())))
                .assertNext(saved -> assertEquals("Updated merchant", saved.getBusinessName()))
                .verifyComplete();
    }

    @Test
    void shouldPreserveRawLifecycleSnapshotsWhenGenericBatchSaveUpdatesMetadata() {
        RewardBatch batch = batch("batch-snapshot-protection");

        StepVerifier.create(adapter.createOrRead(batch)
                        .flatMap(created -> databaseClient()
                                .sql("""
                                        UPDATE reward_batches
                                        SET initial_amount_cents_at_send = 1234,
                                            suspended_amount_cents_at_approving = 567
                                        WHERE id = 'batch-snapshot-protection'
                                        """)
                                .fetch()
                                .rowsUpdated()
                                .then(adapter.findById(created.getId())))
                        .flatMap(loaded -> {
                            loaded.setBusinessName("Updated merchant");
                            loaded.setFilename("updated.csv");
                            return adapter.save(loaded);
                        })
                        .flatMap(saved -> databaseClient()
                                .sql("""
                                        SELECT business_name, filename,
                                               initial_amount_cents_at_send,
                                               suspended_amount_cents_at_approving
                                        FROM reward_batches
                                        WHERE id = :id
                                        """)
                                .bind("id", saved.getId())
                                .map((row, metadata) -> new PersistedBatchValues(
                                        row.get("business_name", String.class),
                                        row.get("filename", String.class),
                                        row.get("initial_amount_cents_at_send", Long.class),
                                        row.get("suspended_amount_cents_at_approving", Long.class)
                                ))
                                .one()))
                .expectNext(new PersistedBatchValues("Updated merchant", "updated.csv", 1234L, 567L))
                .verifyComplete();
    }

    @Test
    void shouldAtomicallySendNonEmptyBatchUsingAssignedRewardSum() {
        RewardBatch earlierEmpty = sendableBatch("send-earlier-empty", 2);
        RewardBatch batch = sendableBatch("send-non-empty", 1);

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(earlierEmpty),
                        adapter.createOrRead(batch)
                )
                .then(insertTransaction("send-transaction-1", batch.getId(), 250L))
                .then(insertTransaction("send-transaction-2", batch.getId(), 450L))
                .then(adapter.sendBatch(batch.getId(), batch.getInitiativeId(), batch.getMerchantId())))
                .assertNext(sent -> {
                    assertEquals(RewardBatchStatus.SENT, sent.getStatus());
                    assertNotNull(sent.getMerchantSendDate());
                    assertEquals(sent.getMerchantSendDate(), sent.getUpdateDate());
                })
                .verifyComplete();

        StepVerifier.create(sendState(batch.getId()))
                .assertNext(persisted -> {
                    assertEquals(RewardBatchStatus.SENT.name(), persisted.status());
                    assertEquals(700L, persisted.initialAmountCentsAtSend());
                    assertNotNull(persisted.merchantSendDate());
                    assertEquals(persisted.merchantSendDate(), persisted.updateDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldSendEmptyBatchWithZeroSnapshot() {
        RewardBatch batch = sendableBatch("send-empty", 1);

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(adapter.sendBatch(batch.getId(), batch.getInitiativeId(), batch.getMerchantId()))
                        .then(sendState(batch.getId())))
                .assertNext(persisted -> {
                    assertEquals(RewardBatchStatus.SENT.name(), persisted.status());
                    assertEquals(0L, persisted.initialAmountCentsAtSend());
                    assertNotNull(persisted.merchantSendDate());
                    assertEquals(persisted.merchantSendDate(), persisted.updateDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectMissingScopeAndWrongMerchantWithoutMutatingTheBatch() {
        RewardBatch batch = sendableBatch("send-owner-check", 1);

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(adapter.sendBatch("missing", batch.getInitiativeId(), batch.getMerchantId())))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.NOT_FOUND, REWARD_BATCH_NOT_FOUND))
                .verify();

        StepVerifier.create(adapter.sendBatch(batch.getId(), "other-initiative", batch.getMerchantId()))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.NOT_FOUND, REWARD_BATCH_NOT_FOUND))
                .verify();

        StepVerifier.create(adapter.sendBatch(batch.getId(), batch.getInitiativeId(), "other-merchant"))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.NOT_FOUND, REWARD_BATCH_NOT_FOUND))
                .verify();

        StepVerifier.create(sendState(batch.getId()))
                .assertNext(SqlRewardBatchAdapterTest::assertUnsent)
                .verifyComplete();
    }

    @Test
    void shouldRejectBlankOrNullIdentifiersAsNotFound() {
        StepVerifier.create(adapter.sendBatch(null, "initiative-1", "merchant-1"))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.NOT_FOUND, REWARD_BATCH_NOT_FOUND))
                .verify();

        StepVerifier.create(adapter.sendBatch(" ", "initiative-1", "merchant-1"))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.NOT_FOUND, REWARD_BATCH_NOT_FOUND))
                .verify();

        StepVerifier.create(adapter.sendBatch("batch", null, "merchant-1"))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.NOT_FOUND, REWARD_BATCH_NOT_FOUND))
                .verify();
    }

    @Test
    void shouldRejectCurrentOrFutureMonthAndInvalidStatusWithoutPartialLifecycleWrites() {
        RewardBatch currentMonth = batch("send-current-month");
        currentMonth.setMonth(YearMonth.now(ZONEID).toString());
        RewardBatch futureMonth = batch("send-future-month");
        futureMonth.setMonth(YearMonth.now(ZONEID).plusMonths(1).toString());
        RewardBatch invalidStatus = sendableBatch("send-invalid-status", 1);
        invalidStatus.setStatus(RewardBatchStatus.EVALUATING);

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(currentMonth),
                        adapter.createOrRead(futureMonth),
                        adapter.createOrRead(invalidStatus)
                ).then(setSendSnapshot(invalidStatus.getId(), 123L)))
                .verifyComplete();

        StepVerifier.create(adapter.sendBatch(
                        currentMonth.getId(), currentMonth.getInitiativeId(), currentMonth.getMerchantId()))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.BAD_REQUEST, REWARD_BATCH_MONTH_TOO_EARLY))
                .verify();
        StepVerifier.create(sendState(currentMonth.getId()))
                .assertNext(SqlRewardBatchAdapterTest::assertUnsent)
                .verifyComplete();

        StepVerifier.create(adapter.sendBatch(
                        futureMonth.getId(), futureMonth.getInitiativeId(), futureMonth.getMerchantId()))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.BAD_REQUEST, REWARD_BATCH_MONTH_TOO_EARLY))
                .verify();
        StepVerifier.create(sendState(futureMonth.getId()))
                .assertNext(SqlRewardBatchAdapterTest::assertUnsent)
                .verifyComplete();

        StepVerifier.create(adapter.sendBatch(
                        invalidStatus.getId(), invalidStatus.getInitiativeId(), invalidStatus.getMerchantId()))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.BAD_REQUEST, REWARD_BATCH_INVALID_REQUEST))
                .verify();
        StepVerifier.create(sendState(invalidStatus.getId()))
                .expectNext(new SendState(RewardBatchStatus.EVALUATING.name(), 123L, null,
                        invalidStatus.getUpdateDate()))
                .verifyComplete();
    }

    @Test
    void shouldFailExplicitlyWhenNonCreatedBatchLacksSendSnapshot() {
        RewardBatch batch = sendableBatch("send-invalid-status-without-snapshot", 1);
        batch.setStatus(RewardBatchStatus.EVALUATING);

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(adapter.sendBatch(batch.getId(), batch.getInitiativeId(), batch.getMerchantId())))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("without initial send snapshot"))
                .verify();
    }

    @Test
    void shouldRejectWhenEarlierCreatedBatchHasAssignedTransactions() {
        RewardBatch earlier = sendableBatch("send-earlier-non-empty", 2);
        RewardBatch current = sendableBatch("send-current", 1);

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(earlier),
                        adapter.createOrRead(current)
                )
                .then(insertTransaction("send-earlier-transaction", earlier.getId(), 99L))
                .then(adapter.sendBatch(
                        current.getId(), current.getInitiativeId(), current.getMerchantId())))
                .expectErrorSatisfies(error ->
                        assertRewardBatchError(error, HttpStatus.BAD_REQUEST, REWARD_BATCH_PREVIOUS_NOT_SENT))
                .verify();

        StepVerifier.create(sendState(current.getId()))
                .assertNext(SqlRewardBatchAdapterTest::assertUnsent)
                .verifyComplete();
    }

    @Test
    void shouldReturnSentBatchIdempotentlyWithoutOverwritingItsSnapshotOrDates() {
        RewardBatch batch = sendableBatch("send-idempotent", 1);

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(insertTransaction("send-idempotent-first", batch.getId(), 300L))
                        .then(adapter.sendBatch(batch.getId(), batch.getInitiativeId(), batch.getMerchantId()))
                        .then(sendState(batch.getId()))
                        .flatMap(firstSend -> insertTransaction(
                                        "send-idempotent-late", batch.getId(), 900L)
                                .then(adapter.sendBatch(
                                        batch.getId(), batch.getInitiativeId(), batch.getMerchantId()))
                                .flatMap(retryResult -> sendState(batch.getId())
                                        .map(retry -> new SendAttempt(firstSend, retry, retryResult)))))
                .assertNext(attempt -> {
                    assertEquals(RewardBatchStatus.SENT, attempt.retryResult().getStatus());
                    assertEquals(300L, attempt.first().initialAmountCentsAtSend());
                    assertEquals(attempt.first(), attempt.retry());
                })
                .verifyComplete();
    }

    @Test
    void shouldFailExplicitlyForMissingOrInconsistentSendSnapshots() {
        RewardBatch sentWithoutSnapshot = sendableBatch("send-missing-snapshot", 2);
        sentWithoutSnapshot.setStatus(RewardBatchStatus.SENT);
        RewardBatch createdWithSnapshot = sendableBatch("send-inconsistent-snapshot", 1);

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(sentWithoutSnapshot),
                        adapter.createOrRead(createdWithSnapshot)
                ).then(setSendSnapshot(createdWithSnapshot.getId(), 10L)))
                .verifyComplete();

        StepVerifier.create(adapter.sendBatch(
                        sentWithoutSnapshot.getId(),
                        sentWithoutSnapshot.getInitiativeId(),
                        sentWithoutSnapshot.getMerchantId()))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("SENT without initial send snapshot"))
                .verify();
        StepVerifier.create(adapter.sendBatch(
                        createdWithSnapshot.getId(),
                        createdWithSnapshot.getInitiativeId(),
                        createdWithSnapshot.getMerchantId()))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("CREATED with an initial send snapshot"))
                .verify();

        StepVerifier.create(Mono.zip(
                        sendState(sentWithoutSnapshot.getId()),
                        sendState(createdWithSnapshot.getId())
                ))
                .assertNext(states -> {
                    assertEquals(new SendState(RewardBatchStatus.SENT.name(), null, null,
                            sentWithoutSnapshot.getUpdateDate()), states.getT1());
                    assertEquals(new SendState(RewardBatchStatus.CREATED.name(), 10L, null,
                            createdWithSnapshot.getUpdateDate()), states.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldSerializeConcurrentSendAttemptsAndPersistOneSnapshot() {
        RewardBatch batch = sendableBatch("send-concurrent", 1);

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(insertTransaction("send-concurrent-transaction", batch.getId(), 550L))
                        .thenMany(Flux.merge(
                                Mono.defer(() -> adapter.sendBatch(
                                        batch.getId(), batch.getInitiativeId(), batch.getMerchantId())),
                                Mono.defer(() -> adapter.sendBatch(
                                        batch.getId(), batch.getInitiativeId(), batch.getMerchantId()))
                        ))
                        .collectList())
                .assertNext(results -> {
                    assertEquals(2, results.size());
                    assertTrue(results.stream()
                            .allMatch(result -> result.getStatus() == RewardBatchStatus.SENT));
                })
                .verifyComplete();

        StepVerifier.create(sendState(batch.getId()))
                .assertNext(persisted -> {
                    assertEquals(RewardBatchStatus.SENT.name(), persisted.status());
                    assertEquals(550L, persisted.initialAmountCentsAtSend());
                    assertNotNull(persisted.merchantSendDate());
                    assertEquals(persisted.merchantSendDate(), persisted.updateDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldAtomicallyCaptureSignedSuspendedRewardsWhenEnteringApproval() {
        RewardBatch batch = approvalBatch("approval-signed");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 1_000L))
                        .then(insertTransaction(
                                "approval-suspended-positive",
                                batch.getId(),
                                "SUSPENDED",
                                250L
                        ))
                        .then(insertTransaction(
                                "approval-suspended-negative",
                                batch.getId(),
                                "SUSPENDED",
                                -75L
                        ))
                        .then(insertTransaction(
                                "approval-consultable",
                                batch.getId(),
                                "CONSULTABLE",
                                900L
                        ))
                        .then(adapter.enterApproval(batch.getId(), batch.getInitiativeId())))
                .assertNext(entered -> {
                    assertEquals(RewardBatchStatus.APPROVING, entered.getStatus());
                    assertNotNull(entered.getApprovalDate());
                    assertNotNull(entered.getUpdateDate());
                })
                .verifyComplete();

        StepVerifier.create(approvalState(batch.getId()))
                .assertNext(state -> {
                    assertEquals(RewardBatchStatus.APPROVING.name(), state.status());
                    assertEquals(1_000L, state.initialAmountCentsAtSend());
                    assertEquals(175L, state.suspendedAmountCentsAtApproving());
                    assertNotNull(state.approvalDate());
                    assertEquals(state.approvalDate(), state.updateDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldCaptureZeroSuspendedAmountForAnEmptyBatch() {
        RewardBatch batch = approvalBatch("approval-empty");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 0L))
                        .then(adapter.enterApproval(batch.getId(), batch.getInitiativeId())))
                .assertNext(entered -> assertEquals(RewardBatchStatus.APPROVING, entered.getStatus()))
                .verifyComplete();

        StepVerifier.create(approvalState(batch.getId()))
                .assertNext(state -> {
                    assertEquals(0L, state.suspendedAmountCentsAtApproving());
                    assertEquals(RewardBatchStatus.APPROVING.name(), state.status());
                })
                .verifyComplete();
    }

    @Test
    void shouldCaptureZeroSuspendedAmountWhenAssignedRowsAreNotSuspended() {
        RewardBatch batch = approvalBatch("approval-no-suspended-rows");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 300L))
                        .then(insertTransaction(
                                "approval-consultable-only",
                                batch.getId(),
                                "CONSULTABLE",
                                300L
                        ))
                        .then(insertTransaction(
                                "approval-approved-only",
                                batch.getId(),
                                "APPROVED",
                                -50L
                        ))
                        .then(adapter.enterApproval(batch.getId(), batch.getInitiativeId())))
                .assertNext(entered -> assertEquals(RewardBatchStatus.APPROVING, entered.getStatus()))
                .verifyComplete();

        StepVerifier.create(approvalState(batch.getId()))
                .assertNext(state -> assertEquals(0L, state.suspendedAmountCentsAtApproving()))
                .verifyComplete();
    }

    @ParameterizedTest
    @EnumSource(
            value = RewardBatchStatus.class,
            names = {"APPROVED", "PENDING_REFUND", "NOT_REFUNDED", "REFUNDED"}
    )
    void shouldAllowApprovalWhenEveryEarlierBatchIsInAnAllowedState(RewardBatchStatus previousStatus) {
        RewardBatch previous = approvalBatch("approval-previous-" + previousStatus.name().toLowerCase());
        previous.setMonth("2026-06");
        previous.setStatus(previousStatus);
        RewardBatch current = approvalBatch("approval-current-" + previousStatus.name().toLowerCase());

        StepVerifier.create(adapter.createOrRead(previous)
                        .then(adapter.createOrRead(current))
                        .then(setSnapshots(previous.getId(), 1L, 2L))
                        .then(setSendSnapshot(current.getId(), 10L))
                        .then(adapter.enterApproval(current.getId(), current.getInitiativeId())))
                .assertNext(entered -> assertEquals(RewardBatchStatus.APPROVING, entered.getStatus()))
                .verifyComplete();

        StepVerifier.create(approvalState(current.getId()))
                .assertNext(state -> assertEquals(0L, state.suspendedAmountCentsAtApproving()))
                .verifyComplete();
    }

    @Test
    void shouldIgnoreEarlierBatchesOutsideTheMerchantInitiativeAndPosGrouping() {
        RewardBatch differentMerchant = approvalBatch("approval-other-merchant");
        differentMerchant.setMonth("2026-06");
        differentMerchant.setMerchantId("other-merchant");

        RewardBatch differentInitiative = approvalBatch("approval-other-initiative");
        differentInitiative.setMonth("2026-05");
        differentInitiative.setInitiativeId("other-initiative");

        RewardBatch differentPos = approvalBatch("approval-other-pos");
        differentPos.setMonth("2026-04");
        differentPos.setPosType(PosType.ONLINE);

        RewardBatch current = approvalBatch("approval-grouping-current");

        StepVerifier.create(Flux.concat(
                                adapter.createOrRead(differentMerchant),
                                adapter.createOrRead(differentInitiative),
                                adapter.createOrRead(differentPos),
                                adapter.createOrRead(current)
                        )
                        .then(setSendSnapshot(current.getId(), 10L))
                        .then(adapter.enterApproval(current.getId(), current.getInitiativeId())))
                .assertNext(entered -> assertEquals(RewardBatchStatus.APPROVING, entered.getStatus()))
                .verifyComplete();
    }

    @Test
    void shouldRejectAnEarlierBatchOutsideTheAllowedFinalApprovalStatesWithoutWriting() {
        RewardBatch previous = approvalBatch("approval-blocking-previous");
        previous.setMonth("2026-06");
        previous.setStatus(RewardBatchStatus.CREATED);
        RewardBatch current = approvalBatch("approval-blocked-current");

        StepVerifier.create(adapter.createOrRead(previous)
                        .then(adapter.createOrRead(current))
                        .then(setSendSnapshot(current.getId(), 10L))
                        .then(adapter.enterApproval(current.getId(), current.getInitiativeId())))
                .expectErrorSatisfies(error -> assertApprovalRequestError(error, HttpStatus.BAD_REQUEST))
                .verify();

        StepVerifier.create(approvalState(current.getId()))
                .assertNext(state -> {
                    assertEquals(RewardBatchStatus.EVALUATING.name(), state.status());
                    assertEquals(10L, state.initialAmountCentsAtSend());
                    assertNull(state.suspendedAmountCentsAtApproving());
                    assertNull(state.approvalDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectNonEvaluatingApprovalWithoutPartialLifecycleWrites() {
        RewardBatch batch = approvalBatch("approval-invalid-status");
        batch.setStatus(RewardBatchStatus.SENT);

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 10L))
                        .then(adapter.enterApproval(batch.getId(), batch.getInitiativeId())))
                .expectErrorSatisfies(error -> assertApprovalRequestError(error, HttpStatus.BAD_REQUEST))
                .verify();

        StepVerifier.create(approvalState(batch.getId()))
                .assertNext(state -> {
                    assertEquals(RewardBatchStatus.SENT.name(), state.status());
                    assertEquals(10L, state.initialAmountCentsAtSend());
                    assertNull(state.suspendedAmountCentsAtApproving());
                    assertNull(state.approvalDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectMissingApprovalScopeBeforeOpeningTheMutation() {
        RewardBatch batch = approvalBatch("approval-scope");

        StepVerifier.create(adapter.createOrRead(batch).then())
                .verifyComplete();

        StepVerifier.create(adapter.enterApproval(null, batch.getInitiativeId()))
                .expectErrorSatisfies(error -> assertApprovalRequestError(
                        error,
                        HttpStatus.NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND
                ))
                .verify();
        StepVerifier.create(adapter.enterApproval(batch.getId(), "other-initiative"))
                .expectErrorSatisfies(error -> assertApprovalRequestError(
                        error,
                        HttpStatus.NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND
                ))
                .verify();

        StepVerifier.create(approvalState(batch.getId()))
                .assertNext(state -> {
                    assertEquals(RewardBatchStatus.EVALUATING.name(), state.status());
                    assertNull(state.initialAmountCentsAtSend());
                    assertNull(state.suspendedAmountCentsAtApproving());
                })
                .verifyComplete();
    }

    @Test
    void shouldRejectApprovalByANonL3AssigneeWithoutPartialLifecycleWrites() {
        RewardBatch batch = approvalBatch("approval-invalid-assignee");
        batch.setAssigneeLevel(RewardBatchAssignee.L2);

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 10L))
                        .then(adapter.enterApproval(batch.getId(), batch.getInitiativeId())))
                .expectErrorSatisfies(error -> assertApprovalRequestError(error, HttpStatus.BAD_REQUEST))
                .verify();

        StepVerifier.create(approvalState(batch.getId()))
                .assertNext(state -> {
                    assertEquals(RewardBatchStatus.EVALUATING.name(), state.status());
                    assertEquals(10L, state.initialAmountCentsAtSend());
                    assertNull(state.suspendedAmountCentsAtApproving());
                    assertNull(state.approvalDate());
                })
                .verifyComplete();
    }

    @Test
    void shouldFailClosedForMissingOrInconsistentApprovalSnapshots() {
        RewardBatch missingSendSnapshot = approvalBatch("approval-missing-send-snapshot");
        RewardBatch missingApprovalSnapshot = approvalBatch("approval-missing-approval-snapshot");
        missingApprovalSnapshot.setMonth("2026-08");
        missingApprovalSnapshot.setStatus(RewardBatchStatus.APPROVING);
        RewardBatch inconsistentSnapshot = approvalBatch("approval-inconsistent-snapshot");
        inconsistentSnapshot.setMonth("2026-09");

        StepVerifier.create(Flux.concat(
                                adapter.createOrRead(missingSendSnapshot),
                                adapter.createOrRead(missingApprovalSnapshot),
                                adapter.createOrRead(inconsistentSnapshot)
                        )
                        .then(setSendSnapshot(missingApprovalSnapshot.getId(), 10L))
                        .then(setSnapshots(inconsistentSnapshot.getId(), 10L, 20L)))
                .verifyComplete();

        StepVerifier.create(adapter.enterApproval(
                        missingSendSnapshot.getId(),
                        missingSendSnapshot.getInitiativeId()))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("without initial send snapshot"))
                .verify();
        StepVerifier.create(adapter.enterApproval(
                        missingApprovalSnapshot.getId(),
                        missingApprovalSnapshot.getInitiativeId()))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("without suspended approval snapshot"))
                .verify();
        StepVerifier.create(adapter.enterApproval(
                        inconsistentSnapshot.getId(),
                        inconsistentSnapshot.getInitiativeId()))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("with a suspended approval snapshot"))
                .verify();

        StepVerifier.create(Mono.zip(
                        approvalState(missingSendSnapshot.getId()),
                        approvalState(missingApprovalSnapshot.getId()),
                        approvalState(inconsistentSnapshot.getId())
                ))
                .assertNext(states -> {
                    assertEquals(RewardBatchStatus.EVALUATING.name(), states.getT1().status());
                    assertNull(states.getT1().initialAmountCentsAtSend());
                    assertNull(states.getT1().suspendedAmountCentsAtApproving());
                    assertEquals(RewardBatchStatus.APPROVING.name(), states.getT2().status());
                    assertEquals(10L, states.getT2().initialAmountCentsAtSend());
                    assertNull(states.getT2().suspendedAmountCentsAtApproving());
                    assertEquals(RewardBatchStatus.EVALUATING.name(), states.getT3().status());
                    assertEquals(20L, states.getT3().suspendedAmountCentsAtApproving());
                })
                .verifyComplete();
    }

    @Test
    void shouldReturnTheCapturedApprovalSnapshotOnRetryWithoutOverwritingIt() {
        RewardBatch batch = approvalBatch("approval-idempotent");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 100L))
                        .then(insertTransaction(
                                "approval-idempotent-first",
                                batch.getId(),
                                "SUSPENDED",
                                300L
                        ))
                        .then(adapter.enterApproval(batch.getId(), batch.getInitiativeId()))
                        .flatMap(first -> approvalState(batch.getId())
                                .flatMap(firstState -> insertTransaction(
                                                "approval-idempotent-late",
                                                batch.getId(),
                                                "SUSPENDED",
                                                900L
                                        )
                                        .then(adapter.enterApproval(
                                                batch.getId(),
                                                batch.getInitiativeId()
                                        ))
                                        .flatMap(retried -> approvalState(batch.getId())
                                                .map(retriedState -> new ApprovalAttempt(
                                                        first,
                                                        retried,
                                                        firstState,
                                                        retriedState
                                                ))))))
                .assertNext(attempt -> {
                    assertEquals(RewardBatchStatus.APPROVING, attempt.firstResult().getStatus());
                    assertEquals(RewardBatchStatus.APPROVING, attempt.retryResult().getStatus());
                    assertEquals(300L, attempt.firstState().suspendedAmountCentsAtApproving());
                    assertEquals(attempt.firstState(), attempt.retryState());
                })
                .verifyComplete();
    }

    @Test
    void shouldSerializeConcurrentApprovalRequestsAndPersistOneSnapshot() {
        RewardBatch batch = approvalBatch("approval-concurrent");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 100L))
                        .then(insertTransaction(
                                "approval-concurrent-transaction",
                                batch.getId(),
                                "SUSPENDED",
                                550L
                        ))
                        .thenMany(Flux.merge(
                                Mono.defer(() -> adapter.enterApproval(
                                        batch.getId(),
                                        batch.getInitiativeId()
                                )),
                                Mono.defer(() -> adapter.enterApproval(
                                        batch.getId(),
                                        batch.getInitiativeId()
                                ))
                        ).collectList()))
                .assertNext(results -> {
                    assertEquals(2, results.size());
                    assertTrue(results.stream()
                            .allMatch(result -> result.getStatus() == RewardBatchStatus.APPROVING));
                })
                .verifyComplete();

        StepVerifier.create(approvalState(batch.getId()))
                .assertNext(state -> assertEquals(550L, state.suspendedAmountCentsAtApproving()))
                .verifyComplete();
    }

    @Test
    void shouldSerializeApprovalEntryWithAConcurrentTransactionDecision() {
        RewardBatch batch = approvalBatch("approval-decision-race");

        StepVerifier.create(adapter.createOrRead(batch)
                        .then(setSendSnapshot(batch.getId(), 100L))
                        .then(insertTransaction(
                                "approval-decision-race-transaction",
                                batch.getId(),
                                "SUSPENDED",
                                400L
                        ))
                        .thenMany(Flux.merge(
                                Mono.defer(() -> adapter.enterApproval(
                                        batch.getId(),
                                        batch.getInitiativeId()
                                )),
                                Mono.defer(() -> evaluationAdapter.updateStatusAndReturnOld(
                                                batch.getInitiativeId(),
                                                batch.getId(),
                                                "approval-decision-race-transaction",
                                                RewardBatchTrxStatus.APPROVED,
                                                null,
                                                batch.getMonth(),
                                                null
                                        ))
                        ))
                        .then(Mono.zip(
                                approvalState(batch.getId()),
                                transactionBatchStatus("approval-decision-race-transaction")
                        )))
                .assertNext(result -> {
                    ApprovalState state = result.getT1();
                    String transactionStatus = result.getT2();
                    assertEquals(RewardBatchStatus.APPROVING.name(), state.status());
                    assertTrue(
                            (state.suspendedAmountCentsAtApproving() == 0L
                                    && "APPROVED".equals(transactionStatus))
                                    || (state.suspendedAmountCentsAtApproving() == 400L
                                    && "SUSPENDED".equals(transactionStatus))
                    );
                })
                .verifyComplete();
    }

    @Test
    void shouldRollBackApprovalStatusAndSnapshotWhenTheMutationCannotPersist() {
        RewardBatch batch = approvalBatch("approval-rollback");

        try {
            StepVerifier.create(adapter.createOrRead(batch)
                            .then(setSendSnapshot(batch.getId(), 100L))
                            .then(insertTransaction(
                                    "approval-rollback-transaction",
                                    batch.getId(),
                                    "SUSPENDED",
                                    400L
                            ))
                            .then(installApprovalMutationFailureTrigger())
                            .then(adapter.enterApproval(batch.getId(), batch.getInitiativeId())))
                    .expectErrorMatches(error -> hasMessage(error, "forced approval mutation failure"))
                    .verify();

            StepVerifier.create(approvalState(batch.getId()))
                    .assertNext(state -> {
                        assertEquals(RewardBatchStatus.EVALUATING.name(), state.status());
                        assertEquals(100L, state.initialAmountCentsAtSend());
                        assertNull(state.suspendedAmountCentsAtApproving());
                        assertNull(state.approvalDate());
                        assertEquals(batch.getUpdateDate(), state.updateDate());
                    })
                    .verifyComplete();
        } finally {
            dropApprovalMutationFailureTrigger().block();
        }
    }

    @Test
    void shouldUpdateStatusAndMetadataWithoutChangingBatchIdentity() {
        RewardBatch batch = batch("batch-metadata");
        DeliveryOutcomeDTO deliveryOutcome = DeliveryOutcomeDTO.builder()
                .idRichiesta("delivery-request")
                .succeded(true)
                .message("accepted")
                .code(200)
                .build();

        StepVerifier.create(adapter.createOrRead(batch)
                        .flatMap(created -> {
                            created.setStatus(RewardBatchStatus.SENT);
                            return adapter.save(created);
                        })
                        .flatMap(updated -> {
                            updated.setFilename("approved.csv");
                            updated.setReportPath("initiative/initiative-1/batch/approved.csv");
                            updated.setMerchantSendDate(LocalDateTime.of(2026, Month.JULY, 1, 10, 30));
                            updated.setDeliveryOutcome(deliveryOutcome);
                            updated.setAssigneeLevel(RewardBatchAssignee.L2);
                            return adapter.updateMetadata(updated);
                        }))
                .assertNext(updated -> {
                    assertEquals(batch.getId(), updated.getId());
                    assertEquals(batch.getInitiativeId(), updated.getInitiativeId());
                    assertEquals(RewardBatchStatus.SENT, updated.getStatus());
                    assertEquals("approved.csv", updated.getFilename());
                    assertEquals("initiative/initiative-1/batch/approved.csv", updated.getReportPath());
                    assertEquals(deliveryOutcome.getIdRichiesta(), updated.getDeliveryOutcome().getIdRichiesta());
                    assertEquals(deliveryOutcome.isSucceded(), updated.getDeliveryOutcome().isSucceded());
                    assertEquals(RewardBatchAssignee.L2, updated.getAssigneeLevel());
                })
                .verifyComplete();
    }

    @Test
    void shouldProjectDerivedCountersAndVirtualStatusesInDatabase() {
        RewardBatch toApprove = batch("batch-to-approve");
        toApprove.setStatus(RewardBatchStatus.EVALUATING);
        toApprove.setAssigneeLevel(RewardBatchAssignee.L3);

        StepVerifier.create(adapter.createOrRead(toApprove)
                        .then(setSnapshots("batch-to-approve", 1_000L, 200L))
                        .thenMany(databaseClient()
                                .sql("""
                                        INSERT INTO reward_transactions (
                                            transaction_id, initiative_id, reward_batch_id,
                                            reward_batch_trx_status, accrued_reward_cents
                                        )
                                        VALUES
                                            ('transaction-check', 'initiative-1', 'batch-to-approve', 'TO_CHECK', 100),
                                            ('transaction-approved', 'initiative-1', 'batch-to-approve', 'APPROVED', 300),
                                            ('transaction-rejected', 'initiative-1', 'batch-to-approve', 'REJECTED', 400),
                                            ('transaction-suspended', 'initiative-1', 'batch-to-approve', 'SUSPENDED', 200)
                                        """)
                                .fetch()
                                .rowsUpdated())
                        .thenMany(listAdapter.findRewardBatches(
                                "merchant-1",
                                "initiative-1",
                                RewardBatchStatus.TO_APPROVE.name(),
                                null,
                                "2026-07",
                                true,
                                PageRequest.of(0, 10)
                        )))
                .assertNext(projected -> {
                    assertEquals(4L, projected.getNumberOfTransactions());
                    assertEquals(1_000L, projected.getInitialAmountCents());
                    assertEquals(3L, projected.getNumberOfTransactionsElaborated());
                    assertEquals(1L, projected.getNumberOfTransactionsSuspended());
                    assertEquals(1L, projected.getNumberOfTransactionsRejected());
                    assertEquals(200L, projected.getSuspendedAmountCents());
                    assertEquals(400L, projected.getApprovedAmountCents());
                })
                .verifyComplete();

        StepVerifier.create(listAdapter.countRewardBatches(
                        "merchant-1",
                        "initiative-1",
                        RewardBatchStatus.TO_WORK.name(),
                        null,
                        "2026-07",
                        true
                ))
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void shouldSelectDeliverableAndPendingRefundBatchesWithDatabasePaging() {
        RewardBatch deliverable = batch("batch-deliverable");
        deliverable.setStatus(RewardBatchStatus.APPROVED);
        deliverable.setMonth("2026-05");
        RewardBatch zeroAmount = batch("batch-zero");
        zeroAmount.setStatus(RewardBatchStatus.APPROVED);
        zeroAmount.setMonth("2026-06");
        RewardBatch pendingRefund = batch("batch-pending-refund");
        pendingRefund.setStatus(RewardBatchStatus.PENDING_REFUND);

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(deliverable),
                        adapter.createOrRead(zeroAmount),
                        adapter.createOrRead(pendingRefund)
                ).then(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, reward_batch_id,
                                    reward_batch_trx_status, accrued_reward_cents
                                )
                                VALUES ('deliverable-transaction', 'initiative-1', 'batch-deliverable', 'APPROVED', 1)
                                """)
                        .fetch()
                        .rowsUpdated())
                                .thenMany(Flux.concat(
                                        setSnapshots("batch-deliverable", 0L, 0L),
                                        setSnapshots("batch-zero", 0L, 0L),
                                        setSnapshots("batch-pending-refund", 0L, 0L)
                                ))
                                .thenMany(listAdapter.findDeliverableBatches(
                                "initiative-1",
                                PageRequest.of(0, 1)
                        )))
                .assertNext(projected -> assertEquals("batch-deliverable", projected.getId()))
                .verifyComplete();

        StepVerifier.create(listAdapter.findBatchesToProcessAfter(
                        RewardBatchStatus.APPROVED, "initiative-1", null, 1))
                .expectNextMatches(projected -> projected.getId().equals("batch-deliverable"))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatchesToProcessAfter(
                        RewardBatchStatus.APPROVED, "initiative-1", "batch-deliverable", 1))
                .verifyComplete();

        StepVerifier.create(listAdapter.findOutcomeBatches(
                        "initiative-1",
                        PageRequest.of(0, 10)
                ))
                .expectNextMatches(projected -> projected.getId().equals("batch-pending-refund"))
                .verifyComplete();
    }

    @Test
    void shouldReadSingleStatusAndMerchantBatchesOnlyWithinTheirDatabaseScopes() {
        RewardBatch scoped = batch("batch-scoped");
        scoped.setMonth("2026-06");
        scoped.setStatus(RewardBatchStatus.SENT);
        RewardBatch earlierSent = batch("batch-earlier-sent");
        earlierSent.setMonth("2026-05");
        earlierSent.setStatus(RewardBatchStatus.SENT);
        RewardBatch merchantCreated = batch("batch-merchant-created");
        merchantCreated.setMonth("2026-04");
        RewardBatch otherMerchant = batch("batch-other-merchant");
        otherMerchant.setMerchantId("other-merchant");
        RewardBatch otherInitiative = batch("batch-other-initiative");
        otherInitiative.setInitiativeId("other-initiative");
        otherInitiative.setStatus(RewardBatchStatus.SENT);

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(scoped),
                        adapter.createOrRead(earlierSent),
                        adapter.createOrRead(merchantCreated),
                        adapter.createOrRead(otherMerchant),
                        adapter.createOrRead(otherInitiative)
                ).thenMany(Flux.concat(
                        setSnapshots("batch-scoped", 0L, 0L),
                        setSnapshots("batch-earlier-sent", 0L, 0L),
                        setSnapshots("batch-other-initiative", 0L, 0L)
                )).then())
                .verifyComplete();

        StepVerifier.create(listAdapter.findBatchesToProcessAfter(
                        RewardBatchStatus.SENT, "initiative-1", null, 1))
                .expectNextMatches(projected -> projected.getId().equals("batch-earlier-sent"))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatchesToProcessAfter(
                        RewardBatchStatus.SENT, "initiative-1", "batch-earlier-sent", 1))
                .expectNextMatches(projected -> projected.getId().equals("batch-scoped"))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatchesToProcessAfter(
                        RewardBatchStatus.SENT, "initiative-1", "batch-scoped", 1))
                .verifyComplete();

        StepVerifier.create(listAdapter.findBatch("batch-scoped"))
                .assertNext(projected -> assertEquals("batch-scoped", projected.getId()))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatch("batch-scoped", "initiative-1"))
                .assertNext(projected -> assertEquals("batch-scoped", projected.getId()))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatch("batch-scoped", "other-initiative"))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatchWithStatus(
                        "batch-scoped", "initiative-1", RewardBatchStatus.SENT))
                .assertNext(projected -> assertEquals("batch-scoped", projected.getId()))
                .verifyComplete();
        StepVerifier.create(listAdapter.findBatchWithStatus(
                        "batch-scoped", "initiative-1", RewardBatchStatus.CREATED))
                .verifyComplete();

        StepVerifier.create(listAdapter.findBatchesWithStatus(
                        RewardBatchStatus.SENT,
                        "initiative-1",
                        PageRequest.of(0, 1, Sort.by(Sort.Direction.ASC, "month"))
                ))
                .assertNext(projected -> assertEquals("batch-earlier-sent", projected.getId()))
                .verifyComplete();

        StepVerifier.create(listAdapter.findMerchantBatch(
                        "merchant-1", "initiative-1", "batch-scoped"))
                .assertNext(projected -> assertEquals("batch-scoped", projected.getId()))
                .verifyComplete();
        StepVerifier.create(listAdapter.findMerchantBatch(
                        "other-merchant", "initiative-1", "batch-scoped"))
                .verifyComplete();
        StepVerifier.create(listAdapter.findMerchantBatches(
                        "merchant-1", "initiative-1", PosType.PHYSICAL)
                        .map(RewardBatch::getId)
                        .collectList())
                .assertNext(ids -> assertEquals(List.of(
                        "batch-merchant-created", "batch-earlier-sent", "batch-scoped"
                ), ids))
                .verifyComplete();
    }

    @Test
    void shouldFindPriorBatchesForMerchantChronology() {
        RewardBatch prior = batch("batch-prior");
        prior.setMonth("2026-05");
        RewardBatch current = batch("batch-current");
        current.setMonth("2026-07");

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(prior),
                        adapter.createOrRead(current)
                ).thenMany(listAdapter.findBatchesBeforeMonth(
                                "merchant-1", "initiative-1", PosType.PHYSICAL, "2026-07"
                        ).map(RewardBatch::getId).collectList()))
                .assertNext(ids -> assertEquals(java.util.List.of("batch-prior"), ids))
                .verifyComplete();
    }

    @Test
    void shouldApplyVisibilityVirtualStatusAndSupportedDatabaseSorts() {
        RewardBatch created = batch("batch-created");
        RewardBatch toWork = batch("batch-to-work");
        toWork.setMonth("2026-06");
        toWork.setStatus(RewardBatchStatus.EVALUATING);
        RewardBatch toApprove = batch("batch-to-approve-virtual");
        toApprove.setMonth("2026-05");
        toApprove.setStatus(RewardBatchStatus.EVALUATING);
        toApprove.setAssigneeLevel(RewardBatchAssignee.L3);
        RewardBatch sent = batch("batch-sent");
        sent.setMonth("2026-04");
        sent.setStatus(RewardBatchStatus.SENT);

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(created),
                        adapter.createOrRead(toWork),
                        adapter.createOrRead(toApprove),
                        adapter.createOrRead(sent)
                )
                .thenMany(Flux.concat(
                        setSnapshots("batch-to-work", 0L, 0L),
                        setSnapshots("batch-to-approve-virtual", 0L, 0L),
                        setSnapshots("batch-sent", 0L, 0L)
                ))
                .thenMany(listAdapter.findRewardBatches(
                        null, null, null, null, null, true, PageRequest.of(0, 10)
                ))
                .map(RewardBatch::getId)
                .collectList())
                .assertNext(ids -> {
                    assertTrue(!ids.contains("batch-created"));
                    assertEquals(java.util.List.of(
                            "batch-sent", "batch-to-approve-virtual", "batch-to-work"
                    ), ids);
                })
                .verifyComplete();

        StepVerifier.create(listAdapter.findRewardBatches(
                        null,
                        "initiative-1",
                        RewardBatchStatus.TO_WORK.name(),
                        RewardBatchAssignee.L1.name(),
                        null,
                        true,
                        null
                ))
                .expectNextMatches(result -> result.getId().equals("batch-to-work"))
                .verifyComplete();

        StepVerifier.create(listAdapter.findRewardBatches(
                        null,
                        "initiative-1",
                        RewardBatchStatus.TO_APPROVE.name(),
                        RewardBatchAssignee.L3.name(),
                        null,
                        true,
                        PageRequest.of(0, 10)
                ))
                .expectNextMatches(result -> result.getId().equals("batch-to-approve-virtual"))
                .verifyComplete();

        StepVerifier.create(listAdapter.countRewardBatches(
                        null,
                        "initiative-1",
                        RewardBatchStatus.TO_WORK.name(),
                        RewardBatchAssignee.L3.name(),
                        null,
                        true
                ))
                .expectNext(0L)
                .verifyComplete();

        StepVerifier.create(listAdapter.countRewardBatches(
                        null,
                        "initiative-1",
                        RewardBatchStatus.CREATED.name(),
                        null,
                        null,
                        true
                ))
                .expectNext(0L)
                .verifyComplete();

        StepVerifier.create(listAdapter.countRewardBatches(
                        null,
                        "initiative-1",
                        null,
                        "not-an-assignee",
                        null,
                        false
                ))
                .expectNext(4L)
                .verifyComplete();

        StepVerifier.create(listAdapter.findBatchesWithStatus(RewardBatchStatus.SENT, "initiative-1"))
                .expectNextMatches(result -> result.getId().equals("batch-sent"))
                .verifyComplete();

        StepVerifier.create(listAdapter.findRewardBatches(
                        null,
                        "initiative-1",
                        RewardBatchStatus.SENT.name(),
                        null,
                        null,
                        true,
                        PageRequest.of(0, 10)
                ))
                .expectNextMatches(result -> result.getId().equals("batch-sent"))
                .verifyComplete();

        StepVerifier.create(Flux.concat(
                        Flux.fromIterable(SqlRewardBatchListAdapter.supportedSortProperties()),
                        Flux.just("unsupported")
                )
                .concatMap(property -> listAdapter.findRewardBatches(
                        null,
                        "initiative-1",
                        null,
                        null,
                        null,
                        false,
                        PageRequest.of(0, 1, Sort.by(Sort.Direction.DESC, property))
                ).single()))
                .expectNextCount(SqlRewardBatchListAdapter.supportedSortProperties().size() + 1)
                .verifyComplete();
    }

    private static RewardBatch batch(String id) {
        return RewardBatch.builder()
                .id(id)
                .initiativeId("initiative-1")
                .merchantId("merchant-1")
                .businessName("Merchant")
                .month("2026-07")
                .posType(PosType.PHYSICAL)
                .status(RewardBatchStatus.CREATED)
                .partial(false)
                .name("Luglio 2026")
                .startDate(LocalDateTime.of(2026, Month.JULY, 1, 0, 0))
                .endDate(LocalDateTime.of(2026, Month.JULY, 31, 23, 59, 59))
                .creationDate(LocalDateTime.of(2026, Month.JULY, 1, 0, 0))
                .updateDate(LocalDateTime.of(2026, Month.JULY, 1, 0, 0))
                .assigneeLevel(RewardBatchAssignee.L1)
                .build();
    }

    private static RewardBatch approvalBatch(String id) {
        RewardBatch batch = batch(id);
        batch.setStatus(RewardBatchStatus.EVALUATING);
        batch.setAssigneeLevel(RewardBatchAssignee.L3);
        return batch;
    }

    private static RewardBatch sendableBatch(String id, long monthsAgo) {
        RewardBatch batch = batch(id);
        YearMonth month = YearMonth.now(ZONEID).minusMonths(monthsAgo);
        batch.setMonth(month.toString());
        batch.setName(month.toString());
        return batch;
    }

    private static Mono<Void> insertTransaction(String transactionId, String batchId, long accruedRewardCents) {
        return insertTransaction(transactionId, batchId, "CONSULTABLE", accruedRewardCents);
    }

    private static Mono<Void> insertTransaction(
            String transactionId,
            String batchId,
            String batchTransactionStatus,
            long accruedRewardCents
    ) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, reward_batch_id,
                            reward_batch_trx_status, accrued_reward_cents
                        )
                        VALUES (:transactionId, 'initiative-1', :batchId, :batchTransactionStatus, :accruedRewardCents)
                        """)
                .bind("transactionId", transactionId)
                .bind("batchId", batchId)
                .bind("batchTransactionStatus", batchTransactionStatus)
                .bind("accruedRewardCents", accruedRewardCents)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<ApprovalState> approvalState(String id) {
        return databaseClient()
                .sql("""
                        SELECT status, initial_amount_cents_at_send,
                               suspended_amount_cents_at_approving, approval_date, update_date
                        FROM reward_batches
                        WHERE id = :id
                        """)
                .bind("id", id)
                .map((row, metadata) -> new ApprovalState(
                        row.get("status", String.class),
                        row.get("initial_amount_cents_at_send", Long.class),
                        row.get("suspended_amount_cents_at_approving", Long.class),
                        row.get("approval_date", LocalDateTime.class),
                        row.get("update_date", LocalDateTime.class)
                ))
                .one();
    }

    private static Mono<String> transactionBatchStatus(String transactionId) {
        return databaseClient()
                .sql("""
                        SELECT reward_batch_trx_status
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", transactionId)
                .map((row, metadata) -> row.get("reward_batch_trx_status", String.class))
                .one();
    }

    private static Mono<Void> setSendSnapshot(String id, long initialAmountCentsAtSend) {
        return databaseClient()
                .sql("""
                        UPDATE reward_batches
                        SET initial_amount_cents_at_send = :initialAmountCentsAtSend
                        WHERE id = :id
                        """)
                .bind("id", id)
                .bind("initialAmountCentsAtSend", initialAmountCentsAtSend)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> installApprovalMutationFailureTrigger() {
        return databaseClient()
                .sql("""
                        CREATE OR REPLACE FUNCTION fail_reward_batch_approval_mutation()
                        RETURNS trigger AS $$
                        BEGIN
                            IF NEW.status = 'APPROVING' AND OLD.status = 'EVALUATING' THEN
                                RAISE EXCEPTION 'forced approval mutation failure';
                            END IF;
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql
                        """)
                .then()
                .then(databaseClient()
                        .sql("""
                                CREATE TRIGGER fail_reward_batch_approval_mutation
                                BEFORE UPDATE ON reward_batches
                                FOR EACH ROW EXECUTE FUNCTION fail_reward_batch_approval_mutation()
                                """)
                        .then());
    }

    private static Mono<Void> dropApprovalMutationFailureTrigger() {
        return databaseClient()
                .sql("DROP TRIGGER IF EXISTS fail_reward_batch_approval_mutation ON reward_batches")
                .then()
                .then(databaseClient()
                        .sql("DROP FUNCTION IF EXISTS fail_reward_batch_approval_mutation()")
                        .then());
    }

    private static Mono<SendState> sendState(String id) {
        return databaseClient()
                .sql("""
                        SELECT status, initial_amount_cents_at_send, merchant_send_date, update_date
                        FROM reward_batches
                        WHERE id = :id
                        """)
                .bind("id", id)
                .map((row, metadata) -> new SendState(
                        row.get("status", String.class),
                        row.get("initial_amount_cents_at_send", Long.class),
                        row.get("merchant_send_date", LocalDateTime.class),
                        row.get("update_date", LocalDateTime.class)
                ))
                .one();
    }

    private static void assertRewardBatchError(Throwable error, HttpStatus status, String code) {
        RewardBatchException exception = assertInstanceOf(RewardBatchException.class, error);
        assertEquals(status, exception.getHttpStatus());
        assertEquals(code, exception.getMessage());
    }

    private static void assertApprovalRequestError(Throwable error, HttpStatus status) {
        assertApprovalRequestError(error, status, REWARD_BATCH_INVALID_REQUEST);
    }

    private static void assertApprovalRequestError(Throwable error, HttpStatus status, String code) {
        ClientExceptionWithBody exception = assertInstanceOf(ClientExceptionWithBody.class, error);
        assertEquals(status, exception.getHttpStatus());
        assertEquals(code, exception.getCode());
    }

    private static boolean hasMessage(Throwable error, String expectedMessage) {
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expectedMessage)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static void assertUnsent(SendState state) {
        assertEquals(RewardBatchStatus.CREATED.name(), state.status());
        assertNull(state.initialAmountCentsAtSend());
        assertNull(state.merchantSendDate());
    }

    private static Mono<Void> setSnapshots(String id, long initialAmountCentsAtSend,
                                           long suspendedAmountCentsAtApproving) {
        return databaseClient()
                .sql("""
                        UPDATE reward_batches
                        SET initial_amount_cents_at_send = :initialAmountCentsAtSend,
                            suspended_amount_cents_at_approving = :suspendedAmountCentsAtApproving
                        WHERE id = :id
                        """)
                .bind("id", id)
                .bind("initialAmountCentsAtSend", initialAmountCentsAtSend)
                .bind("suspendedAmountCentsAtApproving", suspendedAmountCentsAtApproving)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private record PersistedBatchValues(
            String businessName,
            String filename,
            Long initialAmountCentsAtSend,
            Long suspendedAmountCentsAtApproving
    ) {
    }

    private record SendState(
            String status,
            Long initialAmountCentsAtSend,
            LocalDateTime merchantSendDate,
            LocalDateTime updateDate
    ) {
    }

    private record SendAttempt(SendState first, SendState retry, RewardBatch retryResult) {
    }

    private record ApprovalState(
            String status,
            Long initialAmountCentsAtSend,
            Long suspendedAmountCentsAtApproving,
            LocalDateTime approvalDate,
            LocalDateTime updateDate
    ) {
    }

    private record ApprovalAttempt(
            RewardBatch firstResult,
            RewardBatch retryResult,
            ApprovalState firstState,
            ApprovalState retryState
    ) {
    }
}
