package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.Map;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlInvoicedTransactionAssignmentAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String MERCHANT_ID = "merchant-1";

    private static SqlInvoicedTransactionAssignmentAdapter adapter;
    private static DSLContext dslContext;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        dslContext = DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                SQLDialect.POSTGRES
        );
        RewardBatchSqlMapper batchMapper = new RewardBatchSqlMapper(JsonMapper.builder().build());
        RewardTransactionSqlMapper transactionMapper = new RewardTransactionSqlMapper(JsonMapper.builder().build());
        SqlRewardBatchAdapter batchAdapter = new SqlRewardBatchAdapter(
                transactionalOperator(),
                dslContext,
                new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                        .getRepository(RewardBatchSqlRepository.class),
                batchMapper
        );
        SqlRewardTransactionAdapter transactionAdapter = new SqlRewardTransactionAdapter(
                transactionalOperator(),
                dslContext,
                transactionMapper
        );
        adapter = new SqlInvoicedTransactionAssignmentAdapter(
                transactionalOperator(),
                dslContext,
                connectionFactory(),
                batchAdapter,
                transactionAdapter,
                batchMapper,
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
    void shouldAssignAnEligibleTransactionAndClearPriorAssignmentMetadata() {
        RewardTransaction transaction = transaction("transaction-assigned", 750L);
        transaction.setRewardBatchRejectionReason(List.of(new ReasonDTO(null, "obsolete")));

        StepVerifier.create(adapter.assignInvoicedTransaction(transaction, batch(), 123))
                .assertNext(assigned -> {
                    assertNotNull(assigned.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, assigned.getRewardBatchTrxStatus());
                    assertNotNull(assigned.getRewardBatchInclusionDate());
                    assertEquals(123, assigned.getSamplingKey());
                    assertNull(assigned.getRewardBatchRejectionReason());
                })
                .verifyComplete();

        StepVerifier.create(adapter.findInvoicedTransactionWithoutBatch(transaction.getId()))
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_BATCHES)
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(INITIATIVE_ID))))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
    }

    @Test
    void shouldNotCreateABatchWhenStaleInvoicedInputResolvesToRefundedTransaction() {
        RewardTransaction staleInvoiced = transaction("transaction-stale-refunded", 750L);
        staleInvoiced.setTransactionRevision(1L);

        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, status, accrued_reward_cents, transaction_revision
                                )
                                VALUES (
                                    'transaction-stale-refunded', 'initiative-1', 'REFUNDED', 750, 2
                                )
                                """)
                        .fetch()
                        .rowsUpdated()
                        .then(adapter.assignInvoicedTransaction(staleInvoiced, batch(), 123)))
                .assertNext(persisted -> {
                    assertEquals(SyncTrxStatus.REFUNDED.name(), persisted.getStatus());
                    assertEquals(2L, persisted.getTransactionRevision());
                    assertNull(persisted.getRewardBatchId());
                    assertNull(persisted.getRewardBatchTrxStatus());
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_BATCHES)))
                .expectNextMatches(result -> result.value1() == 0)
                .verifyComplete();
    }

    @Test
    void shouldFindOnlyOrderedInvoicedTransactionsWithoutABatch() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, status, accrued_reward_cents
                                )
                                VALUES
                                    ('candidate-b', 'initiative-1', 'INVOICED', 0),
                                    ('candidate-a', 'initiative-1', 'INVOICED', 0),
                                    ('not-a-candidate', 'initiative-1', 'CANCELLED', 0)
                                """)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(adapter.findInvoicedTransactionsWithoutBatch(2)))
                .assertNext(transaction -> assertEquals("candidate-a", transaction.getId()))
                .assertNext(transaction -> assertEquals("candidate-b", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findInvoicedTransactionWithoutBatch("candidate-a"))
                .assertNext(transaction -> assertEquals("candidate-a", transaction.getId()))
                .verifyComplete();
        StepVerifier.create(adapter.findInvoicedTransactionWithoutBatch("not-a-candidate"))
                .verifyComplete();
    }

    @Test
    void shouldRejectABatchFromAnotherInitiativeBeforePersistingTheTransaction() {
        RewardBatch batch = batch();
        batch.setInitiativeId("other-initiative");

        StepVerifier.create(adapter.assignInvoicedTransaction(
                        transaction("transaction-mismatched-initiative", 750L),
                        batch,
                        123
                ))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains("same initiative"))
                .verify();

        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_TRANSACTIONS)))
                .expectNextMatches(result -> result.value1() == 0)
                .verifyComplete();
    }

    @Test
    void shouldRejectTransactionsWithoutExactlyOneNonBlankInitiative() {
        RewardTransaction withoutInitiative = transaction("transaction-without-initiative", 750L);
        withoutInitiative.setInitiatives(null);
        RewardTransaction withMultipleInitiatives = transaction("transaction-multiple-initiatives", 750L);
        withMultipleInitiatives.setInitiatives(List.of(INITIATIVE_ID, "initiative-2"));
        RewardTransaction withBlankInitiative = transaction("transaction-blank-initiative", 750L);
        withBlankInitiative.setInitiatives(List.of(" "));
        RewardBatch invalidBatch = batch();

        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withoutInitiative, invalidBatch, 123)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withMultipleInitiatives, invalidBatch, 123)
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> adapter.assignInvoicedTransaction(withBlankInitiative, invalidBatch, 123)
        );
    }

    @Test
    void shouldRejectAnAssignmentToANonCreatedBatchWithoutPersistingChanges() {
        RewardBatch batch = batch();
        batch.setId("sent-batch");
        batch.setStatus(RewardBatchStatus.SENT);

        StepVerifier.create(adapter.assignInvoicedTransaction(
                        transaction("transaction-sent-batch", 750L),
                        batch,
                        123
                ))
                .expectErrorMatches(error -> error instanceof ClientExceptionNoBody exception
                        && exception.getHttpStatus() == HttpStatus.BAD_REQUEST)
                .verify();

        StepVerifier.create(Mono.zip(
                        Mono.from(dslContext.selectCount().from(REWARD_TRANSACTIONS))
                                .map(Record1::value1),
                        Mono.from(dslContext.selectCount().from(REWARD_BATCHES))
                                .map(Record1::value1)
                ))
                .assertNext(counts -> {
                    assertEquals(0, counts.getT1());
                    assertEquals(0, counts.getT2());
                })
                .verifyComplete();
    }

    @Test
    void shouldAssignUsingTheProvidedBatchIdentifier() {
        RewardBatch batch = batch();
        batch.setId("provided-batch-id");

        StepVerifier.create(adapter.assignInvoicedTransaction(
                        transaction("transaction-provided-batch", 750L),
                        batch,
                        123
                ))
                .assertNext(assigned -> assertEquals("provided-batch-id", assigned.getRewardBatchId()))
                .verifyComplete();
    }

    @Test
    void shouldKeepTheFirstAssignmentOnRetryAfterBatchLifecycleProgresses() {
        RewardTransaction original = transaction("transaction-retry", 750L);
        RewardTransaction retry = transaction("transaction-retry", 750L);

        StepVerifier.create(adapter.assignInvoicedTransaction(original, batch(), 11)
                        .flatMap(first -> Mono.from(dslContext.update(REWARD_BATCHES)
                                        .set(REWARD_BATCHES.STATUS, "SENT")
                                        .where(REWARD_BATCHES.ID.eq(first.getRewardBatchId())))
                                .then(adapter.assignInvoicedTransaction(retry, batch(), 99))
                                .map(second -> List.of(first, second))))
                .assertNext(assignments -> {
                    RewardTransaction first = assignments.getFirst();
                    RewardTransaction second = assignments.getLast();
                    assertEquals(first.getRewardBatchId(), second.getRewardBatchId());
                    assertEquals(11, second.getSamplingKey());
                    assertEquals(first.getRewardBatchInclusionDate(), second.getRewardBatchInclusionDate());
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq("transaction-retry"))))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_BATCHES)))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
    }

    @Test
    void shouldConcurrentlyAssignTransactionsToOneGroupingBatch() {
        RewardTransaction first = transaction("transaction-concurrent-1", 250L);
        RewardTransaction second = transaction("transaction-concurrent-2", 500L);

        StepVerifier.create(Flux.merge(
                        Mono.defer(() -> adapter.assignInvoicedTransaction(first, batch(), 1)),
                        Mono.defer(() -> adapter.assignInvoicedTransaction(second, batch(), 2))
                ).collectList())
                .assertNext(assignments -> {
                    assertEquals(2, assignments.size());
                    assertEquals(
                            1,
                            assignments.stream().map(RewardTransaction::getRewardBatchId).distinct().count()
                    );
                })
                .verifyComplete();

        StepVerifier.create(Mono.from(dslContext.selectCount().from(REWARD_BATCHES)))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
        StepVerifier.create(Mono.from(dslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                                RewardBatchTrxStatus.CONSULTABLE.name()
                        ))))
                .expectNextMatches(result -> result.value1() == 2)
                .verifyComplete();
        StepVerifier.create(Mono.from(dslContext.select(DSL.sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS))
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.isNotNull())))
                .expectNextMatches(result -> result.value1().longValue() == 750L)
                .verifyComplete();
    }

    private static RewardBatch batch() {
        return RewardBatchFactory.create(
                INITIATIVE_ID,
                MERCHANT_ID,
                PosType.PHYSICAL,
                "2026-07",
                "Business"
        );
    }

    private static RewardTransaction transaction(String id, long accruedRewardCents) {
        return RewardTransaction.builder()
                .id(id)
                .initiatives(List.of(INITIATIVE_ID))
                .merchantId(MERCHANT_ID)
                .pointOfSaleId("pos-1")
                .pointOfSaleType(PosType.PHYSICAL)
                .businessName("Business")
                .status(SyncTrxStatus.INVOICED.name())
                .trxChargeDate(LocalDateTime.of(2026, Month.JULY, 1, 10, 30))
                .invoiceUploadDate(LocalDateTime.of(2026, Month.JULY, 1, 10, 30))
                .rewards(Map.of(INITIATIVE_ID, Reward.builder()
                        .accruedRewardCents(accruedRewardCents)
                        .build()))
                .build();
    }
}
