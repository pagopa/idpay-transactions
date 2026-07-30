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
    void shouldAddTransactionRevisionAndLatestAppliedPaymentImpactRevisionMetadata() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT is_nullable, column_default
                                FROM information_schema.columns
                                WHERE table_name = 'reward_transactions'
                                  AND column_name = 'transaction_revision'
                                """)
                        .map((row, metadata) -> row.get("is_nullable", String.class)
                                + ":" + row.get("column_default", String.class))
                        .one()
                        .zipWith(databaseClient()
                                .sql("""
                                        SELECT is_nullable, column_default
                                        FROM information_schema.columns
                                        WHERE table_name = 'reward_transactions'
                                          AND column_name = 'latest_applied_payment_impact_revision'
                                        """)
                                .map((row, metadata) -> row.get("is_nullable", String.class)
                                        + ":" + row.get("column_default", String.class))
                                .one())
                        .map(result -> new PaymentImpactSchema(result.getT1(), result.getT2())))
                .expectNext(new PaymentImpactSchema("NO:0", "NO:0"))
                .verifyComplete();
    }

    @Test
    void shouldEnforceNonNegativeLatestAppliedPaymentImpactRevision() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT constraint_name
                                FROM information_schema.table_constraints
                                WHERE table_name = 'reward_transactions'
                                  AND constraint_name =
                                      'ck_reward_transactions_latest_impact_revision_non_negative'
                                  AND constraint_type = 'CHECK'
                                """)
                        .map((row, metadata) -> row.get("constraint_name", String.class))
                        .one())
                .expectNext("ck_reward_transactions_latest_impact_revision_non_negative")
                .verifyComplete();
    }

    @Test
    void shouldRejectNegativeTransactionRevisionOnInsert() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, accrued_reward_cents,
                                    transaction_revision
                                )
                                VALUES ('transaction-negative-revision', 'initiative-1', 0, -1)
                                """)
                        .fetch()
                        .rowsUpdated())
                .expectErrorMatches(throwable -> throwable.getMessage()
                        .contains("ck_reward_transactions_revision_non_negative"))
                .verify();
    }

    @Test
    void shouldRejectNegativeLatestAppliedPaymentImpactRevisionOnInsert() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, accrued_reward_cents,
                                    latest_applied_payment_impact_revision
                                )
                                VALUES ('transaction-negative-impact', 'initiative-1', 0, -1)
                                """)
                        .fetch()
                        .rowsUpdated())
                .expectErrorMatches(throwable -> throwable.getMessage()
                        .contains("ck_reward_transactions_latest_impact_revision_non_negative"))
                .verify();
    }

    @Test
    void shouldNotCreateTheRewardBatchImpactInboxTable() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT COUNT(*) AS count
                                FROM information_schema.tables
                                WHERE table_name = 'reward_batch_impact_inbox'
                                """)
                        .map((row, metadata) -> row.get("count", Long.class))
                        .one())
                .expectNext(0L)
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
                                    invoice_data, reward_batch_rejection_reasons, checks_error,
                                    accrued_reward_cents
                                )
                                VALUES (
                                    'transaction-1', 'initiative-1', :rewards, :additionalProperties,
                                    :invoiceData, :rejectionReasons, :checksError, 120
                                )
                                RETURNING
                                    rewards -> 'initiative-1' ->> 'accruedRewardCents' AS accrued_reward_cents,
                                    additional_properties ->> 'productName' AS product_name,
                                    additional_properties ->> 'productGtin' AS product_gtin,
                                    invoice_data ->> 'filename' AS invoice_filename,
                                    invoice_data ->> 'docNumber' AS invoice_doc_number,
                                    reward_batch_rejection_reasons -> 0 ->> 'code' AS rejection_code,
                                    checks_error ->> 'productEligibilityError' AS product_eligibility_error
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
                                row.get("accrued_reward_cents", String.class),
                                row.get("product_name", String.class),
                                row.get("product_gtin", String.class),
                                row.get("invoice_filename", String.class),
                                row.get("invoice_doc_number", String.class),
                                row.get("rejection_code", String.class),
                                row.get("product_eligibility_error", String.class)
                        ))
                        .one())
                .expectNext(new JsonbTransaction(
                        "120",
                        "Coffee machine",
                        "1234567890123",
                        "invoice.pdf",
                        "42",
                        "INVALID_PRODUCT",
                        "true"
                ))
                .verifyComplete();
    }

    private record JsonbTransaction(
            String accruedRewardCents,
            String productName,
            String productGtin,
            String invoiceFilename,
            String invoiceDocNumber,
            String rejectionCode,
            String productEligibilityError
    ) {
    }

    private record PaymentImpactSchema(String revisionColumn, String latestAppliedImpactRevisionColumn) {
    }
}
