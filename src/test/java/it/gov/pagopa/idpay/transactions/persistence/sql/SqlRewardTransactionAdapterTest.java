package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
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
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardTransactionAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlRewardTransactionAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardTransactionAdapter(
                transactionalOperator(),
                DSL.using(
                        new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                        SQLDialect.POSTGRES
                ),
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
                .sql("DELETE FROM reward_batch_impact_inbox")
                .fetch()
                .rowsUpdated()
                .then(databaseClient()
                        .sql("DELETE FROM reward_transactions")
                        .fetch()
                        .rowsUpdated())
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
    void shouldApplyOnlyNewerGenericRevisionsWithoutOverwritingLocalMembership() {
        RewardTransaction original = transaction("transaction-revision", "initiative-1");
        original.setTransactionRevision(1L);
        RewardTransaction newer = transaction("transaction-revision", "initiative-1");
        newer.setTransactionRevision(2L);
        newer.setStatus("INVOICED");
        newer.setAmountCents(2_000L);
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
                    assertEquals("revision-batch", saved.getRewardBatchId());
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, saved.getRewardBatchTrxStatus());
                    assertEquals(77, saved.getSamplingKey());
                })
                .verifyComplete();
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
}
