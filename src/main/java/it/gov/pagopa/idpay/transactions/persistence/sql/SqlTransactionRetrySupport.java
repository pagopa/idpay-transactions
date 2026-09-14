package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.spi.R2dbcException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

final class SqlTransactionRetrySupport {

    private SqlTransactionRetrySupport() {
    }

    static boolean isRetryableConcurrencyFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof R2dbcException exception
                    && ("40001".equals(exception.getSqlState())
                    || "40P01".equals(exception.getSqlState()))) {
                return true;
            }
        }
        return false;
    }

    static <T> Mono<T> retryOnConcurrencyFailure(Mono<T> operation) {
        return operation.retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }
}
