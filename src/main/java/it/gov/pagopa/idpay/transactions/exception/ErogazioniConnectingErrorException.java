package it.gov.pagopa.idpay.transactions.exception;

public class ErogazioniConnectingErrorException extends RuntimeException{

    public ErogazioniConnectingErrorException(String message, Throwable failure) {
        super(message, failure);
    }
}
