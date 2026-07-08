package it.gov.pagopa.idpay.transactions.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class InitiativeClientExceptionTest {

    @Test
    void givenMessageWhenInitiativeClientExceptionThenMessageIsSet() {
        InitiativeClientException exception = new InitiativeClientException("Initiative client error");

        assertEquals("Initiative client error", exception.getMessage());
    }

    @Test
    void givenMessageAndCauseWhenInitiativeClientExceptionThenMessageAndCauseAreSet() {
        Throwable cause = new IllegalStateException("Root cause");

        InitiativeClientException exception = new InitiativeClientException("Initiative client error", cause);

        assertEquals("Initiative client error", exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
