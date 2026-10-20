package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
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
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardTransactionAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlRewardTransactionAdapter adapter;
    private static SqlRewardBatchAdapter batchAdapter;
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
        batchAdapter = new SqlRewardBatchAdapter(
                transactionalOperator(),
                dslContext,
                connectionFactory(),
                new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                        .getRepository(RewardBatchSqlRepository.class),
                new RewardBatchSqlMapper(jsonMapper)
        );
        assignmentAdapter = new SqlInvoicedTransactionAssignmentAdapter(
                transactionalOperator(),
                dslContext,
                connectionFactory(),
                batchAdapter,
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
    void shouldPersistNegativeAccruedRewardThroughGenericUpsert() {
        RewardTransaction transaction = transactionWithAccruedReward(
                "transaction-negative-accrued-reward",
                "initiative-1",
                -6000L
        );
        transaction.setTransactionRevision(1L);

        StepVerifier.create(adapter.upsert(transaction))
                .assertNext(saved -> assertEquals(
                        -6000L,
                        saved.getRewards().get("initiative-1").getAccruedRewardCents()
                ))
                .verifyComplete();

        assertPersistedNegativeAccruedReward(transaction.getId());
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
    void shouldPreserveAssignmentWhenFirstSynchronizationRacesWithAssignment() {
        String transactionId = "transaction-first-sync-race";
        RewardBatch batch = batch("first-sync-race-batch");
        RewardTransaction assigned = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardTransaction synchronizedSnapshot = transactionWithAccruedReward(
                transactionId,
                "initiative-1",
                1_100L
        );
        synchronizedSnapshot.setStatus(SyncTrxStatus.INVOICED.name());
        synchronizedSnapshot.setTransactionRevision(2L);
        synchronizedSnapshot.setAmountCents(2_000L);

        StepVerifier.create(
                        batchAdapter.createOrRead(batch)
                                .thenMany(Flux.merge(
                                        assignmentAdapter.assignInvoicedTransaction(assigned, batch, 77),
                                        adapter.upsert(synchronizedSnapshot)
                                ))
                                .collectList()
                                .then(persistedTransactionState(transactionId))
                )
                .assertNext(state -> {
                    assertEquals(batch.getId(), state.batchId());
                    assertEquals(1_100L, state.accruedRewardCents());
                })
                .verifyComplete();
    }

    @Test
    void shouldDetachMembershipWhenFirstRefundSynchronizationRacesWithAssignment() {
        String transactionId = "transaction-first-refund-race";
        RewardBatch batch = batch("first-refund-race-batch");
        RewardTransaction assigned = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardTransaction refundedSnapshot = transactionWithAccruedReward(
                transactionId,
                "initiative-1",
                900L
        );
        refundedSnapshot.setStatus(SyncTrxStatus.REFUNDED.name());
        refundedSnapshot.setTransactionRevision(2L);

        StepVerifier.create(
                        batchAdapter.createOrRead(batch)
                                .thenMany(Flux.merge(
                                        assignmentAdapter.assignInvoicedTransaction(assigned, batch, 77),
                                        adapter.upsertRefundedAndDetach(refundedSnapshot)
                                ))
                                .collectList()
                                .then(persistedTransactionState(transactionId))
                )
                .assertNext(state -> {
                    assertNull(state.batchId());
                    assertEquals(900L, state.accruedRewardCents());
                })
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
        newer.setRewards(Map.of(
                "initiative-1",
                Reward.builder().accruedRewardCents(1_100L).build()
        ));
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
                    assertEquals(2_000L, saved.getAmountCents());
                    assertEquals(
                            1_100L,
                            saved.getRewards().get("initiative-1").getAccruedRewardCents()
                    );
                    assertEquals("revision-batch", saved.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, saved.getRewardBatchTrxStatus());
                    assertEquals(77, saved.getSamplingKey());
                })
                .verifyComplete();
    }

    @Test
    void shouldAcquireAssignedBatchLockBeforeSynchronizingAccruedReward() {
        String batchId = "generic-lock-batch";
        String transactionId = "generic-lock-transaction";
        RewardTransaction assigned = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardBatch batch = batch(batchId);
        RewardTransaction newer = transactionWithAccruedReward(transactionId, "initiative-1", 1_100L);
        newer.setStatus(SyncTrxStatus.INVOICED.name());
        newer.setTransactionRevision(2L);
        newer.setAmountCents(2_000L);

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(assigned, batch, 77)
                        .then(SqlBatchLockTestSupport.holdBatch(
                                connectionFactory(),
                                batchId,
                                "initiative-1"
                        ).flatMap(batchLock -> {
                            var pending = adapter.upsert(newer).toFuture();
                            return SqlBatchLockTestSupport.blockedByWithin(
                                            connectionFactory(),
                                            batchLock.backendPid()
                                    )
                                    .flatMap(blocked -> {
                                        Mono<Void> evidence = blocked
                                                ? persistedTransactionState(transactionId)
                                                        .doOnNext(state -> {
                                                            assertEquals(750L, state.accruedRewardCents());
                                                            assertEquals(batchId, state.batchId());
                                                        })
                                                        .then()
                                                : Mono.empty();
                                        return evidence
                                                .then(batchLock.release())
                                                .then(Mono.fromFuture(pending))
                                                .map(saved -> {
                                                    assertTrue(blocked,
                                                            "generic synchronization must wait for the batch row lock");
                                                    return saved;
                                                });
                                    })
                                    .onErrorResume(error -> batchLock.release().then(Mono.error(error)));
                        })))
                .assertNext(saved -> {
                    assertEquals(batchId, saved.getRewardBatchId());
                    assertEquals(1_100L,
                            saved.getRewards().get("initiative-1").getAccruedRewardCents());
                    assertEquals(2L, saved.getTransactionRevision());
                })
                .verifyComplete();
    }

    @Test
    void shouldCaptureCommittedRewardUpdateInTheSendSnapshot() {
        String batchId = "reward-update-before-send";
        String transactionId = "reward-update-before-send-transaction";
        RewardTransaction assigned = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardBatch batch = batch(batchId);
        RewardTransaction newer = transactionWithAccruedReward(transactionId, "initiative-1", 1_100L);
        newer.setStatus(SyncTrxStatus.INVOICED.name());
        newer.setTransactionRevision(2L);
        newer.setAmountCents(2_000L);

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(assigned, batch, 77).then())
                .verifyComplete();

        StepVerifier.create(SqlBatchLockTestSupport
                        .holdTransaction(connectionFactory(), transactionId, "initiative-1")
                        .flatMap(transactionLock -> {
                            var pendingUpdate = adapter.upsert(newer).toFuture();
                            return SqlBatchLockTestSupport.blockedByWithin(
                                            connectionFactory(),
                                            transactionLock.backendPid()
                                    )
                                    .flatMap(blocked -> {
                                        assertTrue(
                                                blocked,
                                                "reward synchronization must reach the transaction row after "
                                                        + "acquiring the batch row"
                                        );
                                        var pendingSend = batchAdapter.sendBatch(
                                                batchId,
                                                "initiative-1",
                                                "merchant"
                                        ).toFuture();
                                        return transactionLock.release()
                                                .then(Mono.fromFuture(pendingUpdate))
                                                .then(Mono.fromFuture(pendingSend));
                                    })
                                    .onErrorResume(error -> transactionLock.release()
                                            .then(Mono.error(error)));
                        })
                        .timeout(Duration.ofSeconds(15)))
                .assertNext(sent -> assertEquals(RewardBatchStatus.SENT, sent.getStatus()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(
                        persistedTransactionState(transactionId),
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
                    assertEquals(batchId, result.getT1().batchId());
                    assertEquals(1_100L, result.getT1().accruedRewardCents());
                    assertEquals(1_100L, result.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldRetryWhenMembershipChangesWhileWaitingForTheCurrentBatchLock() {
        String sourceBatchId = "generic-membership-source";
        String targetBatchId = "generic-membership-target";
        String transactionId = "generic-membership-transaction";
        RewardTransaction assigned = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardTransaction newer = transactionWithAccruedReward(transactionId, "initiative-1", 1_100L);
        newer.setStatus(SyncTrxStatus.INVOICED.name());
        newer.setTransactionRevision(2L);

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                assigned,
                                batch(sourceBatchId),
                                77
                        )
                        .then(databaseClient()
                                .sql("""
                                        INSERT INTO reward_batches (
                                            id, initiative_id, merchant_id, month, pos_type,
                                            status, name, assignee_level
                                        )
                                        VALUES (
                                            :targetBatchId, 'initiative-1', 'merchant', '2026-08',
                                            'PHYSICAL', 'CREATED', 'August', 'L1'
                                        )
                                        """)
                                .bind("targetBatchId", targetBatchId)
                                .fetch()
                                .rowsUpdated()
                                .then())
                        .then(SqlBatchLockTestSupport.holdBatchAndMoveTransaction(
                                connectionFactory(),
                                sourceBatchId,
                                targetBatchId,
                                "initiative-1",
                                transactionId
                        ).flatMap(batchLock -> {
                            var pending = adapter.upsert(newer).toFuture();
                            return SqlBatchLockTestSupport.blockedByWithin(
                                            connectionFactory(),
                                            batchLock.backendPid()
                                    )
                                    .flatMap(blocked -> batchLock.commit()
                                            .then(Mono.fromFuture(pending))
                                            .map(saved -> {
                                                assertTrue(blocked,
                                                        "synchronization must wait for the observed batch lock");
                                                return saved;
                                            }))
                                    .onErrorResume(error -> batchLock.release().then(Mono.error(error)));
                        })))
                .assertNext(saved -> {
                    assertEquals(targetBatchId, saved.getRewardBatchId());
                    assertEquals(
                            1_100L,
                            saved.getRewards().get("initiative-1").getAccruedRewardCents()
                    );
                    assertEquals(2L, saved.getTransactionRevision());
                })
                .verifyComplete();
    }

    @Test
    void shouldRevalidateClaimedMembershipBeforeGenericRewardUpdateCanBeSnapshotted() {
        String batchId = "generic-observed-after-assignment";
        String observationGateId = "generic-observation-gate";
        String writeGateId = "generic-write-gate";
        String transactionId = "generic-observed-after-assignment-transaction";
        RewardBatch batch = batch(batchId);
        RewardTransaction assigned = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardTransaction delayedUpdate = transactionWithAccruedReward(
                transactionId,
                "initiative-1",
                1_100L
        );
        delayedUpdate.setStatus(SyncTrxStatus.INVOICED.name());
        delayedUpdate.setTransactionRevision(2L);
        delayedUpdate.setCorrelationId("delayed-generic-sync");

        SqlBatchLockTestSupport.HeldRowLock observationLock = null;
        SqlBatchLockTestSupport.HeldRowLock batchLock = null;
        SqlBatchLockTestSupport.HeldRowLock writeLock = null;
        try {
            StepVerifier.create(prepareMembershipRace(batch, observationGateId, writeGateId))
                    .verifyComplete();

            observationLock = SqlBatchLockTestSupport
                    .holdBatch(connectionFactory(), observationGateId, "initiative-1")
                    .block();
            var delayedSynchronization = adapter.upsert(delayedUpdate).toFuture();

            assertTrue(
                    SqlBatchLockTestSupport.blockedByWithin(
                            connectionFactory(),
                            observationLock.backendPid()
                    ).block(),
                    "synchronization must reach the no-row observation gate"
            );

            StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                            assigned,
                            batch,
                            77
                    ))
                    .assertNext(claimed -> assertEquals(batchId, claimed.getRewardBatchId()))
                    .verifyComplete();

            writeLock = SqlBatchLockTestSupport
                    .holdBatch(connectionFactory(), writeGateId, "initiative-1")
                    .block();
            batchLock = SqlBatchLockTestSupport
                    .holdBatch(connectionFactory(), batchId, "initiative-1")
                    .block();
            observationLock.release().block();

            assertTrue(
                    SqlBatchLockTestSupport.blockedByWithin(
                            connectionFactory(),
                            batchLock.backendPid()
                    ).block(),
                    "delayed synchronization must revalidate and wait for the claimed batch"
            );

            batchLock.release().block();

            assertTrue(SqlBatchLockTestSupport.blockedByWithin(
                            connectionFactory(),
                            writeLock.backendPid()
                    ).block(),
                    "synchronization must reach the guarded reward update while holding the batch"
            );
            var pendingSend = batchAdapter.sendBatch(
                    batchId,
                    "initiative-1",
                    "merchant"
            ).toFuture();
            assertFalse(pendingSend.isDone(), "send completed before synchronization released its batch lock");

            writeLock.release().block();

            StepVerifier.create(Mono.fromFuture(delayedSynchronization))
                    .assertNext(updated -> {
                        assertEquals(batchId, updated.getRewardBatchId());
                        assertEquals(
                                1_100L,
                                updated.getRewards().get("initiative-1").getAccruedRewardCents()
                        );
                    })
                    .verifyComplete();
            StepVerifier.create(Mono.fromFuture(pendingSend))
                    .assertNext(sent -> assertEquals(RewardBatchStatus.SENT, sent.getStatus()))
                    .verifyComplete();

            StepVerifier.create(Mono.zip(
                            persistedTransactionState(transactionId),
                            lifecycleSnapshots(batchId)
                    ))
                    .assertNext(result -> {
                        assertEquals(new TransactionState(batchId, 1_100L), result.getT1());
                        assertEquals(new LifecycleSnapshots(1_100L, null), result.getT2());
                    })
                    .verifyComplete();
        } finally {
            releaseLock(writeLock);
            releaseLock(batchLock);
            releaseLock(observationLock);
            dropMembershipRace().block();
        }
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
    void shouldAcquireCurrentBatchLockBeforeRefundedDetachAndKeepSnapshotsUnchanged() {
        String batchId = "refunded-lock-batch";
        String transactionId = "refunded-lock-transaction";
        RewardTransaction invoiced = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardTransaction refunded = transactionWithAccruedReward(transactionId, "initiative-1", 900L);
        refunded.setStatus(SyncTrxStatus.REFUNDED.name());
        refunded.setTransactionRevision(2L);

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                invoiced,
                                batch(batchId),
                                77
                        )
                        .then(databaseClient()
                                .sql("""
                                        UPDATE reward_batches
                                        SET initial_amount_cents_at_send = 321,
                                            suspended_amount_cents_at_approving = 654
                                        WHERE id = :batchId
                                        """)
                                .bind("batchId", batchId)
                                .fetch()
                                .rowsUpdated()
                                .then())
                        .then(SqlBatchLockTestSupport.holdBatch(
                                connectionFactory(),
                                batchId,
                                "initiative-1"
                        ).flatMap(batchLock -> {
                            var pending = adapter.upsertRefundedAndDetach(refunded).toFuture();
                            return SqlBatchLockTestSupport.blockedByWithin(
                                            connectionFactory(),
                                            batchLock.backendPid()
                                    )
                                    .flatMap(blocked -> {
                                        Mono<Void> evidence = blocked
                                                ? Mono.zip(
                                                                persistedTransactionState(transactionId),
                                                                lifecycleSnapshots(batchId)
                                                        )
                                                        .doOnNext(state -> {
                                                            assertEquals(batchId, state.getT1().batchId());
                                                            assertEquals(
                                                                    new LifecycleSnapshots(321L, 654L),
                                                                    state.getT2()
                                                            );
                                                        })
                                                        .then()
                                                : Mono.empty();
                                        return evidence
                                                .then(batchLock.release())
                                                .then(Mono.fromFuture(pending))
                                                .map(saved -> {
                                                    assertTrue(blocked,
                                                            "REFUNDED detach must wait for the current batch row lock");
                                                    return saved;
                                                });
                                    })
                                    .onErrorResume(error -> batchLock.release().then(Mono.error(error)));
                        })))
                .assertNext(saved -> {
                    assertNull(saved.getRewardBatchId());
                    assertNull(saved.getRewardBatchTrxStatus());
                    assertEquals(2L, saved.getTransactionRevision());
                })
                .verifyComplete();

        StepVerifier.create(lifecycleSnapshots(batchId))
                .expectNext(new LifecycleSnapshots(321L, 654L))
                .verifyComplete();
    }

    @Test
    void shouldRevalidateClaimedMembershipBeforeRefundedDetachCanBeSnapshotted() {
        String batchId = "refunded-observed-after-assignment";
        String observationGateId = "refunded-observation-gate";
        String writeGateId = "refunded-write-gate";
        String transactionId = "refunded-observed-after-assignment-transaction";
        RewardBatch batch = batch(batchId);
        RewardTransaction assigned = invoicedTransaction(transactionId, "initiative-1", 1L);
        RewardTransaction delayedDetach = transactionWithAccruedReward(
                transactionId,
                "initiative-1",
                -6_000L
        );
        delayedDetach.setStatus(SyncTrxStatus.REFUNDED.name());
        delayedDetach.setTransactionRevision(2L);
        delayedDetach.setCorrelationId("delayed-refunded-detach");

        SqlBatchLockTestSupport.HeldRowLock observationLock = null;
        SqlBatchLockTestSupport.HeldRowLock batchLock = null;
        SqlBatchLockTestSupport.HeldRowLock writeLock = null;
        try {
            StepVerifier.create(prepareMembershipRace(batch, observationGateId, writeGateId))
                    .verifyComplete();

            observationLock = SqlBatchLockTestSupport
                    .holdBatch(connectionFactory(), observationGateId, "initiative-1")
                    .block();
            var delayedDetachFuture = adapter.upsertRefundedAndDetach(delayedDetach).toFuture();

            assertTrue(
                    SqlBatchLockTestSupport.blockedByWithin(
                            connectionFactory(),
                            observationLock.backendPid()
                    ).block(),
                    "REFUNDED detach must reach the no-row observation gate"
            );

            StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                            assigned,
                            batch,
                            77
                    ))
                    .assertNext(claimed -> assertEquals(batchId, claimed.getRewardBatchId()))
                    .verifyComplete();

            writeLock = SqlBatchLockTestSupport
                    .holdBatch(connectionFactory(), writeGateId, "initiative-1")
                    .block();
            batchLock = SqlBatchLockTestSupport
                    .holdBatch(connectionFactory(), batchId, "initiative-1")
                    .block();
            observationLock.release().block();

            assertTrue(
                    SqlBatchLockTestSupport.blockedByWithin(
                            connectionFactory(),
                            batchLock.backendPid()
                    ).block(),
                    "REFUNDED detach must revalidate and wait for the claimed batch"
            );

            batchLock.release().block();

            assertTrue(SqlBatchLockTestSupport.blockedByWithin(
                            connectionFactory(),
                            writeLock.backendPid()
                    ).block(),
                    "REFUNDED detach must reach the guarded membership update while holding the batch"
            );
            var pendingSend = batchAdapter.sendBatch(
                    batchId,
                    "initiative-1",
                    "merchant"
            ).toFuture();
            assertFalse(pendingSend.isDone(), "send completed before REFUNDED detach released its batch lock");

            writeLock.release().block();

            StepVerifier.create(Mono.fromFuture(delayedDetachFuture))
                    .assertNext(detached -> {
                        assertEquals(SyncTrxStatus.REFUNDED.name(), detached.getStatus());
                        assertNull(detached.getRewardBatchId());
                        assertNull(detached.getRewardBatchTrxStatus());
                        assertEquals(-6_000L,
                                detached.getRewards().get("initiative-1").getAccruedRewardCents());
                    })
                    .verifyComplete();
            StepVerifier.create(Mono.fromFuture(pendingSend))
                    .assertNext(sent -> assertEquals(RewardBatchStatus.SENT, sent.getStatus()))
                    .verifyComplete();

            StepVerifier.create(Mono.zip(
                            persistedTransactionState(transactionId),
                            lifecycleSnapshots(batchId)
                    ))
                    .assertNext(result -> {
                        assertEquals(new TransactionState(null, -6_000L), result.getT1());
                        assertEquals(new LifecycleSnapshots(0L, null), result.getT2());
                    })
                    .verifyComplete();
        } finally {
            releaseLock(writeLock);
            releaseLock(batchLock);
            releaseLock(observationLock);
            dropMembershipRace().block();
        }
    }

    @Test
    void shouldDetachAssignedMembershipAndPersistNegativeRewardWhenPersistingANewerRefundedSnapshot() {
        String batchId = "refunded-detach-batch";
        RewardTransaction invoiced = invoicedTransaction("transaction-refunded-detach", "initiative-1", 1L);
        ChecksError checksError = new ChecksError(
                true, false, false, false, false, false, false, false
        );
        invoiced.setChecksError(checksError);
        invoiced.setRewardBatchLastMonthElaborated("2025-01");
        RewardTransaction refunded = transactionWithAccruedReward(
                "transaction-refunded-detach",
                "initiative-1",
                -6000L
        );
        refunded.setTransactionRevision(2L);
        refunded.setStatus(SyncTrxStatus.REFUNDED.name());

        StepVerifier.create(assignmentAdapter.assignInvoicedTransaction(
                                invoiced,
                                batch(batchId),
                                77
                        )
                        .then(databaseClient()
                                .sql("""
                                        UPDATE reward_batches
                                        SET initial_amount_cents_at_send = 321,
                                            suspended_amount_cents_at_approving = 654
                                        WHERE id = :batchId
                                        """)
                                .bind("batchId", batchId)
                                .fetch()
                                .rowsUpdated()
                                .then())
                        .then(adapter.upsertRefundedAndDetach(refunded)))
                .assertNext(saved -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), saved.getStatus());
                    assertEquals(2L, saved.getTransactionRevision());
                    assertEquals(
                            -6000L,
                            saved.getRewards().get("initiative-1").getAccruedRewardCents()
                    );
                    assertNull(saved.getRewardBatchId());
                    assertNull(saved.getRewardBatchTrxStatus());
                    assertNull(saved.getRewardBatchInclusionDate());
                    assertEquals(0, saved.getSamplingKey());
                    assertEquals("2025-01", saved.getRewardBatchLastMonthElaborated());
                    assertEquals(checksError, saved.getChecksError());
                })
                .verifyComplete();

        assertPersistedNegativeAccruedReward(refunded.getId());

        StepVerifier.create(Mono.zip(
                        liveBatchAggregate(batchId),
                        lifecycleSnapshots(batchId)
                ))
                .assertNext(result -> {
                    assertEquals(new LiveBatchAggregate(0L, 0L), result.getT1());
                    assertEquals(new LifecycleSnapshots(321L, 654L), result.getT2());
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

    private static RewardTransaction transactionWithAccruedReward(
            String id,
            String initiativeId,
            long accruedRewardCents
    ) {
        RewardTransaction transaction = transaction(id, initiativeId);
        transaction.setRewards(Map.of(
                initiativeId,
                Reward.builder().accruedRewardCents(accruedRewardCents).build()
        ));
        return transaction;
    }

    private static void assertPersistedNegativeAccruedReward(String transactionId) {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT accrued_reward_cents,
                                       rewards -> 'initiative-1' ->> 'accruedRewardCents'
                                           AS json_accrued_reward_cents
                                FROM reward_transactions
                                WHERE transaction_id = :transactionId
                                """)
                        .bind("transactionId", transactionId)
                        .map((row, metadata) -> new PersistedAccruedReward(
                                row.get("accrued_reward_cents", Long.class),
                                row.get("json_accrued_reward_cents", String.class)
                        ))
                        .one())
                .expectNext(new PersistedAccruedReward(-6000L, "-6000"))
                .verifyComplete();
    }

    private static Mono<TransactionState> persistedTransactionState(String transactionId) {
        return databaseClient()
                .sql("""
                        SELECT reward_batch_id, accrued_reward_cents
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", transactionId)
                .map((row, metadata) -> new TransactionState(
                        row.get("reward_batch_id", String.class),
                        row.get("accrued_reward_cents", Long.class)
                ))
                .one();
    }

    private static Mono<LifecycleSnapshots> lifecycleSnapshots(String batchId) {
        return databaseClient()
                .sql("""
                        SELECT initial_amount_cents_at_send, suspended_amount_cents_at_approving
                        FROM reward_batches
                        WHERE id = :batchId
                        """)
                .bind("batchId", batchId)
                .map((row, metadata) -> new LifecycleSnapshots(
                        row.get("initial_amount_cents_at_send", Long.class),
                        row.get("suspended_amount_cents_at_approving", Long.class)
                ))
                .one();
    }

    private static Mono<Void> prepareMembershipRace(
            RewardBatch batch,
            String observationGateId,
            String writeGateId
    ) {
        return batchAdapter.createOrRead(batch)
                .then(insertRaceControlBatch(observationGateId))
                .then(insertRaceControlBatch(writeGateId))
                .then(createMembershipRaceTrigger(observationGateId, writeGateId));
    }

    private static Mono<Void> insertRaceControlBatch(String batchId) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, business_name, month, pos_type,
                            status, name, assignee_level
                        )
                        VALUES (
                            :id, 'initiative-1', :merchantId, 'Control', '2026-08', 'PHYSICAL',
                            'CREATED', 'Control', 'L1'
                        )
                        """)
                .bind("id", batchId)
                .bind("merchantId", "control-" + batchId)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> createMembershipRaceTrigger(
            String observationGateId,
            String writeGateId
    ) {
        return databaseClient()
                .sql("""
                        CREATE OR REPLACE FUNCTION pause_reward_transaction_membership_race()
                        RETURNS trigger AS $$
                        BEGIN
                            IF NEW.correlation_id IN (
                                'delayed-generic-sync',
                                'delayed-refunded-detach'
                            ) AND TG_OP = 'INSERT' THEN
                                PERFORM id
                                FROM reward_batches
                                WHERE id = TG_ARGV[0]
                                  AND initiative_id = 'initiative-1'
                                FOR UPDATE;
                            ELSIF NEW.correlation_id IN (
                                'delayed-generic-sync',
                                'delayed-refunded-detach'
                            ) AND TG_OP = 'UPDATE' THEN
                                PERFORM id
                                FROM reward_batches
                                WHERE id = TG_ARGV[1]
                                  AND initiative_id = 'initiative-1'
                                FOR UPDATE;
                            END IF;
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql
                        """)
                .then()
                .then(databaseClient()
                        .sql("""
                                CREATE TRIGGER pause_reward_transaction_membership_race_trigger
                                BEFORE INSERT OR UPDATE ON reward_transactions
                                FOR EACH ROW
                                EXECUTE FUNCTION pause_reward_transaction_membership_race(
                                    '%s',
                                    '%s'
                                )
                                """.formatted(observationGateId, writeGateId))
                        .then());
    }

    private static Mono<Void> dropMembershipRace() {
        return databaseClient()
                .sql("""
                        DROP TRIGGER IF EXISTS pause_reward_transaction_membership_race_trigger
                        ON reward_transactions
                        """)
                .then()
                .then(databaseClient()
                        .sql("DROP FUNCTION IF EXISTS pause_reward_transaction_membership_race()")
                        .then());
    }

    private static void releaseLock(SqlBatchLockTestSupport.HeldRowLock lock) {
        if (lock != null) {
            lock.release().block();
        }
    }

    private static Mono<LiveBatchAggregate> liveBatchAggregate(String batchId) {
        return databaseClient()
                .sql("""
                        SELECT COUNT(*) AS number_of_transactions,
                               COALESCE(SUM(accrued_reward_cents), 0) AS initial_amount_cents
                        FROM reward_transactions
                        WHERE reward_batch_id = :batchId
                        """)
                .bind("batchId", batchId)
                .map((row, metadata) -> new LiveBatchAggregate(
                        row.get("number_of_transactions", Long.class),
                        row.get("initial_amount_cents", Long.class)
                ))
                .one();
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

    private record PersistedAccruedReward(Long typedAccruedRewardCents, String jsonAccruedRewardCents) {
    }

    private record TransactionState(String batchId, Long accruedRewardCents) {
    }

    private record LifecycleSnapshots(Long initialAmountCentsAtSend, Long suspendedAmountCentsAtApproving) {
    }

    private record LiveBatchAggregate(Long numberOfTransactions, Long initialAmountCents) {
    }
}
