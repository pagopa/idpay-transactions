package it.gov.pagopa.idpay.transactions.config;

public class InitiativeNotFoundException extends RuntimeException {
    public InitiativeNotFoundException(String message) {
        super(message);
    }
}