package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.common.kafka.service.ErrorNotifierService;
import it.gov.pagopa.idpay.transactions.config.KafkaConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionErrorNotifierServiceImplTest {
    private static final String BINDER_KAFKA_TYPE = "kafka";
    private static final String BINDER_BROKER = "broker";
    private static final String DUMMY_MESSAGE = "DUMMY MESSAGE";
    private static final Message<String> dummyMessage = MessageBuilder.withPayload(DUMMY_MESSAGE).build();
    @Mock
    private ErrorNotifierService errorNotifierServiceMock;

    private TransactionErrorNotifierServiceImpl transactionErrorNotifierService;
    @Mock
    private KafkaConfiguration kafkaConfiguration;

    @BeforeEach
    void setUp() {
        transactionErrorNotifierService = new TransactionErrorNotifierServiceImpl(
                kafkaConfiguration, errorNotifierServiceMock
        );
    }

    @Test
    void notifyTransaction() {
        KafkaConfiguration.KafkaInfoDTO kafkaInfoDTO = KafkaConfiguration.KafkaInfoDTO.builder()
                .type(BINDER_KAFKA_TYPE)
                .brokers(BINDER_BROKER)
                .destination("transaction-topic")
                .group("transaction-group")
                .build();
        when(kafkaConfiguration.getStream()).thenReturn(mock(KafkaConfiguration.Stream.class));
        when(kafkaConfiguration.getStream().getBindings()).thenReturn(Map.of("rewardTrxConsumer-in-0",kafkaInfoDTO));
        errorNotifyMock(kafkaInfoDTO,true,true);
        transactionErrorNotifierService.notifyTransaction(dummyMessage,DUMMY_MESSAGE,true,new Throwable(DUMMY_MESSAGE));

        Mockito.verifyNoMoreInteractions(errorNotifierServiceMock);
    }


    @Test
    void notifyTransactionCommands() {
        KafkaConfiguration.KafkaInfoDTO kafkaInfoDTO = KafkaConfiguration.KafkaInfoDTO.builder()
                .type(BINDER_KAFKA_TYPE)
                .brokers(BINDER_BROKER)
                .destination("commands-topic")
                .group("commands-group")
                .build();
        when(kafkaConfiguration.getStream()).thenReturn(mock(KafkaConfiguration.Stream.class));
        when(kafkaConfiguration.getStream().getBindings()).thenReturn(Map.of("consumerCommands-in-0",kafkaInfoDTO));

        errorNotifyMock(kafkaInfoDTO,true,true);
        transactionErrorNotifierService.notifyTransactionCommands(dummyMessage,DUMMY_MESSAGE,true,new Throwable(DUMMY_MESSAGE));

        Mockito.verifyNoMoreInteractions(errorNotifierServiceMock);
    }

    @Test
    void testNotify() {
        KafkaConfiguration.BaseKafkaInfoDTO baseKafkaInfoDTO = KafkaConfiguration.BaseKafkaInfoDTO.builder()
                .type(BINDER_KAFKA_TYPE)
                .brokers(BINDER_BROKER)
                .destination("commands-topic")
                .group("commands-group")
                .build();
        errorNotifyMock(baseKafkaInfoDTO,true,true);
        transactionErrorNotifierService.notify(baseKafkaInfoDTO,dummyMessage,DUMMY_MESSAGE,true,true,new Throwable(DUMMY_MESSAGE));

        Mockito.verifyNoMoreInteractions(errorNotifierServiceMock);
    }

    @Test
    void notifyTransactionOutcome() {
        KafkaConfiguration.KafkaInfoDTO kafkaInfoDTO = KafkaConfiguration.KafkaInfoDTO.builder()
                .type(BINDER_KAFKA_TYPE)
                .brokers(BINDER_BROKER)
                .destination("transaction-outcome-topic")
                .group("transaction-outcome-group")
                .build();

        when(kafkaConfiguration.getStream()).thenReturn(mock(KafkaConfiguration.Stream.class));
        when(kafkaConfiguration.getStream().getBindings())
                .thenReturn(Map.of("transactionOutcome-out-0", kafkaInfoDTO));

        errorNotifyMock(kafkaInfoDTO, true, false);

        transactionErrorNotifierService.notifyTransactionOutcome(
                dummyMessage,
                DUMMY_MESSAGE,
                true,
                new Throwable(DUMMY_MESSAGE)
        );

        Mockito.verifyNoMoreInteractions(errorNotifierServiceMock);
    }


    private void errorNotifyMock(KafkaConfiguration.BaseKafkaInfoDTO baseKafkaInfoDTO,boolean retryable, boolean resendApplication) {
        when(errorNotifierServiceMock.notify(eq(baseKafkaInfoDTO), eq(dummyMessage), eq(DUMMY_MESSAGE), eq(retryable), eq(resendApplication), any()))
                .thenReturn(true);
    }
}