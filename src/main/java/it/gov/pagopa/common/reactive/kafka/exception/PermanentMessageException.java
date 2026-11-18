package it.gov.pagopa.common.reactive.kafka.exception;

public class PermanentMessageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PermanentMessageException(String message) {
        super(message);
    }

    public PermanentMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
