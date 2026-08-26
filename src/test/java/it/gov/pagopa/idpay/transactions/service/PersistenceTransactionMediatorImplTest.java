package it.gov.pagopa.idpay.transactions.service;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.common.kafka.utils.KafkaConstants;
import it.gov.pagopa.common.utils.TestUtils;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardTransactionMapper;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
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
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
        rt1.setStatus(SyncTrxStatus.AUTHORIZED.name());

        Mockito.when(rewardTransactionMapper.mapFromDTO(Mockito.any(RewardTransactionDTO.class)))
                .thenReturn(rt1)
                .thenThrow(new RuntimeException("boom"));

        Mockito.when(rewardTransactionService.save(rt1)).thenReturn(Mono.just(rt1));

        persistenceTransactionMediator.execute(messageFlux);

        Mockito.verify(rewardTransactionMapper, Mockito.timeout(10000).times(2))
                .mapFromDTO(Mockito.any(RewardTransactionDTO.class));
        Mockito.verify(rewardTransactionService, Mockito.timeout(10000).times(1))
                .save(Mockito.any(RewardTransaction.class));
        Mockito.verify(transactionErrorNotifierService, Mockito.timeout(10000).times(1))
                .notifyTransaction(
                        Mockito.any(Message.class),
                        Mockito.anyString(),
                        Mockito.anyBoolean(),
                        Mockito.any(RuntimeException.class)
                );
    }

    @Test
    void executeShouldMapAndSaveInvoicedTransactionWithoutPaymentDeletion() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.INVOICED.name());

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(rt)).thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload").build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionMapper).mapFromDTO(rtDTO);
        verify(rewardTransactionService).save(rt);
    }

    @Test
    void executeShouldMapAndSaveNonInvoicedTransaction() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.AUTHORIZED.name());

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(rt)).thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload").build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionMapper).mapFromDTO(rtDTO);
        verify(rewardTransactionService).save(rt);
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
    void executeShouldPersistMappedRefundedStatusAndIgnoreOperationTypeHeader() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        rtDTO.setStatus(SyncTrxStatus.REFUNDED.name());
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.REFUNDED.name());

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(rt)).thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload")
                                .setHeader("operationType", "REFUNDED")
                                .build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionMapper).mapFromDTO(rtDTO);
        verify(rewardTransactionService).save(rt);
    }

    @Test
    void executeShouldNotRewriteStatusFromOperationTypeHeader() {
        RewardTransactionDTO rtDTO = RewardTransactionDTOFaker.mockInstance(1);
        rtDTO.setStatus(SyncTrxStatus.INVOICED.name());
        RewardTransaction rt = RewardTransactionFaker.mockInstance(1);
        rt.setStatus(SyncTrxStatus.INVOICED.name());

        when(rewardTransactionMapper.mapFromDTO(rtDTO)).thenReturn(rt);
        when(rewardTransactionService.save(rt)).thenReturn(Mono.just(rt));

        StepVerifier.create(persistenceTransactionMediator.execute(
                        rtDTO,
                        MessageBuilder.withPayload("payload")
                                .setHeader("operationType", "REFUNDED")
                                .build(),
                        Map.of()))
                .expectNext(rt)
                .verifyComplete();

        verify(rewardTransactionService).save(argThat(saved ->
                SyncTrxStatus.INVOICED.name().equals(saved.getStatus())));
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
