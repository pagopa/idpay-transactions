package it.gov.pagopa.idpay.transactions.exception;

public class InvitaliaConnectingErrorException extends RuntimeException{

    public InvitaliaConnectingErrorException(String message, Throwable failure) {
        super(message, failure);
    }
}
