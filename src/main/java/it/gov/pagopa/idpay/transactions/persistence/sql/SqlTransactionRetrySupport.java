package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.spi.R2dbcException;

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
}
