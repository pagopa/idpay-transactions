package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.kafka.service.ErrorNotifierService;
import it.gov.pagopa.idpay.transactions.config.KafkaConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TransactionErrorNotifierServiceImpl implements TransactionErrorNotifierService {
    private static final String KAFKA_BINDINGS_TRANSACTIONS = "rewardTrxConsumer-in-0";
    private static final String KAFKA_BINDINGS_TRANSACTIONS_COMMANDS = "consumerCommands-in-0";
    private final ErrorNotifierService errorNotifierService;
    private final KafkaConfiguration kafkaConfiguration;

    public TransactionErrorNotifierServiceImpl(KafkaConfiguration kafkaConfiguration,
                                               ErrorNotifierService errorNotifierService) {
        this.errorNotifierService = errorNotifierService;
        this.kafkaConfiguration = kafkaConfiguration;
    }

    @Override
    public void notifyTransaction(Message<?> message, String description, boolean retryable, Throwable exception) {
        notify(kafkaConfiguration.getStream().getBindings().get(KAFKA_BINDINGS_TRANSACTIONS), message, description, retryable, true, exception);
    }

    @Override
    public void notifyTransactionCommands(Message<String> message, String description, boolean retryable, Throwable exception) {
        notify(kafkaConfiguration.getStream().getBindings().get(KAFKA_BINDINGS_TRANSACTIONS_COMMANDS), message, description, retryable, true, exception);
    }

    @Override
    public void notify(KafkaConfiguration.BaseKafkaInfoDTO baseKafkaInfoDTO, Message<?> message, String description, boolean retryable, boolean resendApplication, Throwable exception) {
        errorNotifierService.notify(baseKafkaInfoDTO, message, description, retryable,resendApplication, exception);
    }
}
