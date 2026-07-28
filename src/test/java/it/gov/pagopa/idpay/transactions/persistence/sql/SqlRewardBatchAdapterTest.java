package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.time.Month;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlRewardBatchAdapter adapter;
    private static SqlRewardBatchListAdapter listAdapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardBatchAdapter(
                transactionalOperator(),
                DSL.using(
                        new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                        SQLDialect.POSTGRES
                ),
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
    void shouldUpdateStatusAndMetadataWithoutChangingBatchIdentity() {
        RewardBatch batch = batch("batch-metadata");
        DeliveryOutcomeDTO deliveryOutcome = DeliveryOutcomeDTO.builder()
                .idRichiesta("delivery-request")
                .succeded(true)
                .message("accepted")
                .code(200)
                .build();

        StepVerifier.create(adapter.createOrRead(batch)
                        .flatMap(created -> adapter.updateStatus(
                                created.getId(),
                                created.getInitiativeId(),
                                RewardBatchStatus.SENT
                        ))
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
                        .thenMany(listAdapter.findDeliverableBatches(
                                "initiative-1",
                                PageRequest.of(0, 1)
                        )))
                .assertNext(projected -> assertEquals("batch-deliverable", projected.getId()))
                .verifyComplete();

        StepVerifier.create(listAdapter.findOutcomeBatches(
                        "initiative-1",
                        PageRequest.of(0, 10)
                ))
                .expectNextMatches(projected -> projected.getId().equals("batch-pending-refund"))
                .verifyComplete();
    }

    @Test
    void shouldFindPriorBatchesAndOnlyReferenceFreeEmptyBatches() {
        RewardBatch prior = batch("batch-prior");
        prior.setMonth("2026-05");
        RewardBatch referencedEmpty = batch("batch-referenced");
        referencedEmpty.setMonth("2026-06");
        RewardBatch eligibleEmpty = batch("batch-eligible");
        eligibleEmpty.setMonth("2026-04");

        StepVerifier.create(Flux.concat(
                        adapter.createOrRead(prior),
                        adapter.createOrRead(referencedEmpty),
                        adapter.createOrRead(eligibleEmpty)
                ).then(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, reward_batch_id, accrued_reward_cents
                                )
                                VALUES ('referenced-transaction', 'initiative-1', 'batch-referenced', 0)
                                """)
                        .fetch()
                        .rowsUpdated())
                        .thenMany(listAdapter.findBatchesBeforeMonth(
                                "merchant-1", "initiative-1", PosType.PHYSICAL, "2026-07"
                        ).map(RewardBatch::getId).collectList()))
                .assertNext(ids -> assertEquals(
                        java.util.List.of("batch-eligible", "batch-prior", "batch-referenced"),
                        ids
                ))
                .verifyComplete();

        StepVerifier.create(listAdapter.findEmptyBatchesBeforeCurrentMonth()
                        .map(RewardBatch::getId)
                        .collectList())
                .assertNext(ids -> {
                    assertTrue(ids.contains("batch-prior"));
                    assertTrue(ids.contains("batch-eligible"));
                    assertTrue(!ids.contains("batch-referenced"));
                })
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
}
