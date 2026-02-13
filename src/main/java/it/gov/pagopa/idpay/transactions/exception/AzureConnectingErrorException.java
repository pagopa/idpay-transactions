package it.gov.pagopa.idpay.transactions.exception;

public class AzureConnectingErrorException extends RuntimeException{

    public AzureConnectingErrorException(String message, Throwable failure) {
        super(message, failure);
    }
}
