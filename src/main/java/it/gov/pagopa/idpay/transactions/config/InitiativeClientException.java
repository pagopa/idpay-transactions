package it.gov.pagopa.idpay.transactions.config;

public class InitiativeClientException extends RuntimeException {
    public InitiativeClientException(String message) {
        super(message);
    }

    public InitiativeClientException(String message, Throwable cause) {
        super(message, cause);
    }
}