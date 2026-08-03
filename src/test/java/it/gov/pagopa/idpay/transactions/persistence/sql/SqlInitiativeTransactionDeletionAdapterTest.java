package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;

import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
class SqlInitiativeTransactionDeletionAdapterTest extends PostgresqlMigrationTestSupport {

    private static SqlInitiativeTransactionDeletionAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlInitiativeTransactionDeletionAdapter(
                transactionalOperator(),
                DSL.using(new TransactionAwareConnectionFactoryProxy(connectionFactory()), SQLDialect.POSTGRES)
        );
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient().sql("DELETE FROM reward_transactions").fetch().rowsUpdated().block();
    }

    @Test
    void deletesOnlyRowsForTheRequestedInitiative() {
        Mono<Void> seed = databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (transaction_id, initiative_id, accrued_reward_cents)
                        VALUES ('transaction-a', 'initiative-a', 0), ('transaction-b', 'initiative-b', 0)
                        """)
                .fetch()
                .rowsUpdated()
                .then();

        StepVerifier.create(seed.then(adapter.deleteTransactions("initiative-a")))
                .expectNext(1L)
                .verifyComplete();

        StepVerifier.create(Mono.from(
                        DSL.using(new TransactionAwareConnectionFactoryProxy(connectionFactory()), SQLDialect.POSTGRES)
                                .selectCount()
                                .from(REWARD_TRANSACTIONS)
                                .where(REWARD_TRANSACTIONS.INITIATIVE_ID.eq("initiative-b"))))
                .expectNextMatches(result -> result.value1() == 1)
                .verifyComplete();
    }
}
