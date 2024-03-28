package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.config.KafkaConfiguration;
import org.springframework.messaging.Message;

public interface TransactionErrorNotifierService {
    void notifyTransaction(Message<?> message, String description, boolean retryable, Throwable exception);
    void notifyTransactionCommands(Message<String> message, String description, boolean retryable, Throwable exception);
    void notify(KafkaConfiguration.BaseKafkaInfoDTO baseKafkaInfoDTO ,Message<?> message, String description, boolean retryable, boolean resendApplication, Throwable exception);
}
