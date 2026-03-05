package it.gov.pagopa.idpay.transactions.exception;

public class SelfcareConnectingErrorException extends RuntimeException{

    public SelfcareConnectingErrorException(String message, Throwable failure) {
        super(message, failure);
    }
}
