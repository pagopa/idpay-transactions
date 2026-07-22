package it.gov.pagopa.idpay.transactions.repository;

import io.r2dbc.postgresql.codec.Json;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
class RewardTransactionsSchemaMigrationTest extends PostgresqlMigrationTestSupport {

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @Test
    void shouldCreateRewardTransactionsTableWithInitiativeSafeBatchForeignKey() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT constraint_name
                                FROM information_schema.table_constraints
                                WHERE table_name = 'reward_transactions'
                                  AND constraint_name = 'fk_reward_transactions_batch_initiative'
                                """)
                        .map((row, metadata) -> row.get("constraint_name", String.class))
                        .one())
                .expectNext("fk_reward_transactions_batch_initiative")
                .verifyComplete();
    }

    @Test
    void shouldRoundTripJsonbDeferredStructuresThroughR2dbcCodec() {
        String additionalProperties = """
                {"productName":"Coffee machine","productGtin":"1234567890123"}
                """;

        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, rewards, additional_properties,
                                    invoice_data, reward_batch_rejection_reasons, checks_error
                                )
                                VALUES (
                                    'transaction-1', 'initiative-1', :rewards, :additionalProperties,
                                    :invoiceData, :rejectionReasons, :checksError
                                )
                                RETURNING rewards, additional_properties, invoice_data,
                                    reward_batch_rejection_reasons, checks_error
                                """)
                        .bind("rewards", Json.of("""
                                {"initiative-1":{"accruedRewardCents":120}}
                                """))
                        .bind("additionalProperties", Json.of(additionalProperties))
                        .bind("invoiceData", Json.of("""
                                {"filename":"invoice.pdf","docNumber":"42"}
                                """))
                        .bind("rejectionReasons", Json.of("""
                                [{"code":"INVALID_PRODUCT"}]
                                """))
                        .bind("checksError", Json.of("""
                                {"productEligibilityError":true}
                                """))
                        .map((row, metadata) -> new JsonbTransaction(
                                row.get("rewards", Json.class).asString(),
                                row.get("additional_properties", Json.class).asString(),
                                row.get("invoice_data", Json.class).asString(),
                                row.get("reward_batch_rejection_reasons", Json.class).asString(),
                                row.get("checks_error", Json.class).asString()
                        ))
                        .one())
                .expectNext(new JsonbTransaction(
                        "{\"initiative-1\":{\"accruedRewardCents\":120}}",
                        additionalProperties.strip(),
                        "{\"filename\":\"invoice.pdf\",\"docNumber\":\"42\"}",
                        "[{\"code\":\"INVALID_PRODUCT\"}]",
                        "{\"productEligibilityError\":true}"
                ))
                .verifyComplete();
    }

    private record JsonbTransaction(
            String rewards,
            String additionalProperties,
            String invoiceData,
            String rejectionReasons,
            String checksError
    ) {
    }
}
