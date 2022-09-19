package it.gov.pagopa.idpay.transactions.exception;

public class NotEnoughFiltersException extends RuntimeException{
    public NotEnoughFiltersException(String message){
        super(message);
    }
}
