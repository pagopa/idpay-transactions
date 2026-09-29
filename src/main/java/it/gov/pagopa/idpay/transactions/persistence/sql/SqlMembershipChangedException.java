package it.gov.pagopa.idpay.transactions.persistence.sql;

final class SqlMembershipChangedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    SqlMembershipChangedException(String message) {
        super(message);
    }
}
