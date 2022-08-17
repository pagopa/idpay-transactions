package it.gov.pagopa.idpay.transactions.service;

import org.springframework.messaging.Message;

public interface ErrorNotifierService {
    void notifyTransaction(Message<?> message, String description, boolean retryable, Throwable exception);
    void notify(String srcServer, String srcTopic, Message<?> message, String description, boolean retryable, Throwable exception);
}
