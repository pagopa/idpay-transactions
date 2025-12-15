package it.gov.pagopa.idpay.transactions.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.gov.pagopa.common.kafka.utils.KafkaConstants;
import it.gov.pagopa.common.utils.TestUtils;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionDTOFaker;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

@ExtendWith(MockitoExtension.class)
class PersistenceTransactionMediatorImplTest {

    @Mock
    private RewardTransactionService rewardTransactionService;

    @Mock
    private TransactionErrorNotifierService transactionErrorNotifierService;

    @Mock
    private RewardTransactionMapper rewardTransactionMapper;

    private PersistenceTransactionMediatorImpl persistenceTransactionMediator;

    @BeforeEach
    void setUp() {
        persistenceTransactionMediator = new PersistenceTransactionMediatorImpl(
                "appName",
                rewardTransactionService,
                transactionErrorNotifierService,
                rewardTransactionMapper,
                1000,
                TestUtils.objectMapper
        );
    }

    @Test
    void execute() {
        RewardTransactionDTO rtDT1 = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransactionDTO rtDT2 = RewardTransactionDTOFaker.mockInstance(2);

        Flux<Message<String>> messageFlux = Flux.just(rtDT1, rtDT2)
                .map(TestUtils::jsonSerializer)
                .map(payload -> MessageBuilder
                        .withPayload(payload)
                        .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                        .setHeader(KafkaHeaders.OFFSET, 0L)
                        .build()
                );

        RewardTransaction rt1 = RewardTransactionFaker.mockInstance(1);

        Mockito.when(rewardTransactionMapper.mapFromDTO(rtDT1)).thenReturn(rt1);
        Mockito.when(rewardTransactionMapper.mapFromDTO(rtDT2)).thenThrow(new RuntimeException("boom"));

        Mockito.when(rewardTransactionService.save(rt1)).thenReturn(Mono.just(rt1));

        persistenceTransactionMediator.execute(messageFlux);

        Mockito.verify(rewardTransactionMapper, Mockito.timeout(1000).times(2))
                .mapFromDTO(Mockito.any(RewardTransactionDTO.class));
        Mockito.verify(rewardTransactionService, Mockito.timeout(1000).times(1))
                .save(Mockito.any(RewardTransaction.class));
        Mockito.verify(transactionErrorNotifierService, Mockito.timeout(1000).times(1))
                .notifyTransaction(Mockito.any(Message.class), Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(RuntimeException.class));
    }

    @Test
    void executeErrorDeserializer() {
        Flux<Message<String>> messageFlux = Flux.just(
                MessageBuilder
                        .withPayload("Error message")
                        .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                        .setHeader(KafkaHeaders.OFFSET, 0L)
                        .build()
        );

        persistenceTransactionMediator.execute(messageFlux);

        Mockito.verifyNoInteractions(rewardTransactionMapper);
        Mockito.verifyNoInteractions(rewardTransactionService);

        Mockito.verify(transactionErrorNotifierService, Mockito.timeout(1000).times(1))
                .notifyTransaction(Mockito.any(Message.class), Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(JsonProcessingException.class));
    }

    @Test
    void otherApplicationRetryTest() {
        RewardTransactionDTO rtDT1 = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransactionDTO rtDT2 = RewardTransactionDTOFaker.mockInstance(2);

        Flux<Message<String>> msgs = Flux.just(rtDT1, rtDT2)
                .map(TestUtils::jsonSerializer)
                .map(payload -> MessageBuilder
                        .withPayload(payload)
                        .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                        .setHeader(KafkaHeaders.OFFSET, 0L)
                )
                .doOnNext(m -> m.setHeader(KafkaConstants.ERROR_MSG_HEADER_APPLICATION_NAME,
                        "otherAppName".getBytes(StandardCharsets.UTF_8)))
                .map(MessageBuilder::build);

        persistenceTransactionMediator.execute(msgs);

        Mockito.verifyNoInteractions(rewardTransactionMapper, rewardTransactionService, transactionErrorNotifierService);
    }

    @Test
    void executeShouldSkipRefundedTransactions() {
        RewardTransactionDTO rtDT = RewardTransactionDTOFaker.mockInstance(1);

        Flux<Message<String>> messageFlux = Flux.just(rtDT)
                .map(TestUtils::jsonSerializer)
                .map(payload -> MessageBuilder
                        .withPayload(payload)
                        .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                        .setHeader(KafkaHeaders.OFFSET, 0L)
                        .setHeader("operationType", "REFUNDED")
                        .build()
                );

        persistenceTransactionMediator.execute(messageFlux);

        Mockito.verifyNoInteractions(rewardTransactionMapper, rewardTransactionService);
        Mockito.verifyNoInteractions(transactionErrorNotifierService);
    }

    @Test
    void getFlowNameShouldReturnTRANSACTION() {
        org.junit.jupiter.api.Assertions.assertEquals("TRANSACTION", persistenceTransactionMediator.getFlowName());
    }

    @Test
    void protectedMethods_shouldBeCovered_viaReflection() {
        Duration d = (Duration) ReflectionTestUtils.invokeMethod(persistenceTransactionMediator, "getCommitDelay");
        org.junit.jupiter.api.Assertions.assertEquals(Duration.ofMillis(1000), d);

        Object reader = ReflectionTestUtils.invokeMethod(persistenceTransactionMediator, "getObjectReader");
        org.junit.jupiter.api.Assertions.assertNotNull(reader);

        ReflectionTestUtils.invokeMethod(
                persistenceTransactionMediator,
                "subscribeAfterCommits",
                Flux.just(List.of(RewardTransactionFaker.mockInstance(1)))
        );
    }

    @Test
    void onDeserializationError_shouldNotifyTransactionErrorNotifierService() {
        Message<String> msg = MessageBuilder.withPayload("bad-json")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.OFFSET, 0L)
                .build();

        Consumer<Throwable> consumer = (Consumer<Throwable>) ReflectionTestUtils.invokeMethod(
                persistenceTransactionMediator,
                "onDeserializationError",
                msg
        );

        RuntimeException ex = new RuntimeException("deserialize");

        consumer.accept(ex);

        Mockito.verify(transactionErrorNotifierService, Mockito.times(1))
                .notifyTransaction(Mockito.eq(msg), Mockito.anyString(), Mockito.eq(true), Mockito.eq(ex));
    }

    @Test
    void notifyError_shouldNotifyTransactionErrorNotifierService() {
        Message<String> msg = MessageBuilder.withPayload("any")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.OFFSET, 0L)
                .build();

        RuntimeException ex = new RuntimeException("handler");

        ReflectionTestUtils.invokeMethod(persistenceTransactionMediator, "notifyError", msg, ex);

        Mockito.verify(transactionErrorNotifierService, Mockito.times(1))
                .notifyTransaction(Mockito.eq(msg), Mockito.anyString(), Mockito.eq(true), Mockito.eq(ex));
    }
}
