package it.gov.pagopa.common.kafka.service;

import it.gov.pagopa.idpay.transactions.config.KafkaConfiguration;
import org.springframework.messaging.Message;

public interface ErrorNotifierService {
    boolean notify(KafkaConfiguration.BaseKafkaInfoDTO baseKafkaInfoDTO, Message<?> message, String description, boolean retryable, boolean resendApplication, Throwable exception);
}