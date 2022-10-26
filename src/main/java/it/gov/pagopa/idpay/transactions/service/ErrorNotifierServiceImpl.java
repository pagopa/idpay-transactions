package it.gov.pagopa.idpay.transactions.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
@Slf4j
public class ErrorNotifierServiceImpl implements ErrorNotifierService {
    public static final String ERROR_MSG_HEADER_APPLICATION_NAME = "applicationName";
    public static final String ERROR_MSG_HEADER_GROUP = "group";

    public static final String ERROR_MSG_HEADER_SRC_TYPE = "srcType";
    public static final String ERROR_MSG_HEADER_SRC_SERVER = "srcServer";
    public static final String ERROR_MSG_HEADER_SRC_TOPIC = "srcTopic";
    public static final String ERROR_MSG_HEADER_DESCRIPTION = "description";
    public static final String ERROR_MSG_HEADER_RETRYABLE = "retryable";
    public static final String ERROR_MSG_HEADER_STACKTRACE = "stacktrace";

    private final StreamBridge streamBridge;
    private final String applicationName;

    private final String trxMessagingServiceType;
    private final String trxServer;
    private final String trxTopic;
    private final String trxGroup;

    public ErrorNotifierServiceImpl(StreamBridge streamBridge,
                                    @Value("${spring.application.name}") String applicationName,

                                    @Value("${spring.cloud.stream.binders.kafka-transactions.type}") String trxMessagingServiceType,
                                    @Value("${spring.cloud.stream.binders.kafka-transactions.environment.spring.cloud.stream.kafka.binder.brokers}") String trxServer,
                                    @Value("${spring.cloud.stream.bindings.rewardTrxConsumer-in-0.destination}") String trxTopic,
                                    @Value("${spring.cloud.stream.bindings.rewardTrxConsumer-in-0.group}") String trxGroup) {
        this.streamBridge = streamBridge;
        this.applicationName = applicationName;

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
    public void notify(String srcType, String srcServer, String srcTopic, String group, Message<?> message, String description, boolean retryable, boolean resendApplication, Throwable exception) {
        log.info("[ERROR_NOTIFIER] notifying error: {}", description, exception);
        final MessageBuilder<?> errorMessage = MessageBuilder.fromMessage(message)
                .setHeader(ERROR_MSG_HEADER_GROUP, group)
                .setHeader(ERROR_MSG_HEADER_SRC_TYPE, srcType)
                .setHeader(ERROR_MSG_HEADER_SRC_SERVER, srcServer)
                .setHeader(ERROR_MSG_HEADER_SRC_TOPIC, srcTopic)
                .setHeader(ERROR_MSG_HEADER_DESCRIPTION, description)
                .setHeader(ERROR_MSG_HEADER_RETRYABLE, retryable)
                .setHeader(ERROR_MSG_HEADER_STACKTRACE, ExceptionUtils.getStackTrace(exception));

        addExceptionInfo(errorMessage, "rootCause", ExceptionUtils.getRootCause(exception));
        addExceptionInfo(errorMessage, "cause", exception.getCause());

        byte[] receivedKey = message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY, byte[].class);
        if(receivedKey!=null){
            errorMessage.setHeader(KafkaHeaders.MESSAGE_KEY, new String(receivedKey, StandardCharsets.UTF_8));
        }

        if (resendApplication){
            errorMessage.setHeader(ERROR_MSG_HEADER_APPLICATION_NAME, applicationName);
        }

        if (!streamBridge.send("errors-out-0", errorMessage.build())) {
            log.error("[ERROR_NOTIFIER] Something gone wrong while notifying error");
        }
    }

    private void addExceptionInfo(MessageBuilder<?> errorMessage, String exceptionHeaderPrefix, Throwable rootCause) {
        errorMessage
                .setHeader("%sClass".formatted(exceptionHeaderPrefix), rootCause != null ? rootCause.getClass().getName() : null)
                .setHeader("%sMessage".formatted(exceptionHeaderPrefix), rootCause != null ? rootCause.getMessage() : null);
    }
}
