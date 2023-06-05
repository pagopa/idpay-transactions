package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.kafka.service.ErrorNotifierService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TransactionErrorNotifierServiceImpl implements TransactionErrorNotifierService {

    private final ErrorNotifierService errorNotifierService;

    private final String trxMessagingServiceType;
    private final String trxServer;
    private final String trxTopic;
    private final String trxGroup;

    public TransactionErrorNotifierServiceImpl(ErrorNotifierService errorNotifierService,

                                               @Value("${spring.cloud.stream.binders.kafka-transactions.type}") String trxMessagingServiceType,
                                               @Value("${spring.cloud.stream.binders.kafka-transactions.environment.spring.cloud.stream.kafka.binder.brokers}") String trxServer,
                                               @Value("${spring.cloud.stream.bindings.rewardTrxConsumer-in-0.destination}") String trxTopic,
                                               @Value("${spring.cloud.stream.bindings.rewardTrxConsumer-in-0.group}") String trxGroup) {
        this.errorNotifierService = errorNotifierService;

        this.trxMessagingServiceType = trxMessagingServiceType;
        this.trxServer = trxServer;
        this.trxTopic = trxTopic;
        this.trxGroup = trxGroup;
    }

    @Override
    public void notifyTransaction(Message<?> message, String description, boolean retryable, Throwable exception) {
        notify(trxMessagingServiceType, trxServer, trxTopic, trxGroup, message, description, retryable, true, exception);
    }

    @Override
    public void notify(String srcType, String srcServer, String srcTopic, String group, Message<?> message, String description, boolean retryable,boolean resendApplication, Throwable exception) {
        errorNotifierService.notify(srcType, srcServer, srcTopic, group, message, description, retryable,resendApplication, exception);
    }
}
