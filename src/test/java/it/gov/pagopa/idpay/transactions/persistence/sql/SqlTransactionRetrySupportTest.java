package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.r2dbc.spi.R2dbcTransientResourceException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class SqlTransactionRetrySupportTest {

    @ParameterizedTest
    @ValueSource(strings = {"40001", "40P01"})
    void shouldClassifyNestedSerializationAndDeadlockFailuresAsRetryable(String sqlState) {
        Throwable failure = new IllegalStateException(
                "transaction failed",
                new R2dbcTransientResourceException("database concurrency failure", sqlState)
        );

        assertTrue(SqlTransactionRetrySupport.isRetryableConcurrencyFailure(failure));
    }

    @ParameterizedTest
    @ValueSource(strings = {"23505", "42P01"})
    void shouldNotClassifyOtherDatabaseFailuresAsRetryable(String sqlState) {
        Throwable failure = new R2dbcTransientResourceException("database failure", sqlState);

        assertFalse(SqlTransactionRetrySupport.isRetryableConcurrencyFailure(failure));
    }

    @Test
    void shouldRetryAnOperationAfterAConcurrencyFailure() {
        AtomicInteger attempts = new AtomicInteger();

        Mono<String> operation = SqlTransactionRetrySupport.retryOnConcurrencyFailure(
                Mono.defer(() -> attempts.incrementAndGet() == 1
                        ? Mono.error(new R2dbcTransientResourceException("serialization failure", "40001"))
                        : Mono.just("sent"))
        );

        StepVerifier.create(operation)
                .expectNext("sent")
                .verifyComplete();

        assertEquals(2, attempts.get());
    }
}
