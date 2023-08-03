package it.gov.pagopa.idpay.transactions.service;

import org.springframework.messaging.Message;

public interface TransactionErrorNotifierService {
    void notifyTransaction(Message<?> message, String description, boolean retryable, Throwable exception);
    @SuppressWarnings("squid:S00107") // suppressing too many parameters alert
    void notify(String srcType, String srcServer, String srcTopic, String group, Message<?> message, String description, boolean retryable, boolean resendApplication, Throwable exception);
    void notifyRewardCommands(Message<String> message, String description, boolean retryable, Throwable exception);
}
