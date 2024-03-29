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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

@ExtendWith(MockitoExtension.class)
class PersistenceTransactionMediatorImplTest {
    @Mock
    private RewardTransactionService rewardTransactionService;

    @Mock
    private TransactionErrorNotifierService transactionErrorNotifierService;

    @Mock
    private RewardTransactionMapper rewardTransactionMapper;

    private PersistenceTransactionMediator persistenceTransactionMediator;

    @BeforeEach
    void setUp() {
        persistenceTransactionMediator = new PersistenceTransactionMediatorImpl(
                "appName",
                rewardTransactionService,
                transactionErrorNotifierService,
                rewardTransactionMapper,
                1000,
                TestUtils.objectMapper);
    }

    @Test
    void execute(){
        // Given
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
        Mockito.when(rewardTransactionMapper.mapFromDTO(rtDT2)).thenThrow(RuntimeException.class);

        Mockito.when(rewardTransactionService.save(rt1)).thenReturn(Mono.just(rt1));

        // When
        persistenceTransactionMediator.execute(messageFlux);

        // Then
        Mockito.verify(rewardTransactionMapper, Mockito.times(2)).mapFromDTO(Mockito.any(RewardTransactionDTO.class));
        Mockito.verify(rewardTransactionService, Mockito.times(1)).save(Mockito.any(RewardTransaction.class));
        Mockito.verify(transactionErrorNotifierService, Mockito.times(1)).notifyTransaction(Mockito.any(Message.class),Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(RuntimeException.class));
    }

    @Test
    void executeErrorDeserializer(){
        // Given
        Flux<Message<String>> messageFlux = Flux.just(MessageBuilder
                .withPayload("Error message")
                .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                .setHeader(KafkaHeaders.OFFSET, 0L)
                .build());

        // When
        persistenceTransactionMediator.execute(messageFlux);

        // Then
        Mockito.verifyNoInteractions(rewardTransactionMapper);
        Mockito.verifyNoInteractions(rewardTransactionService);

        Mockito.verify(transactionErrorNotifierService, Mockito.times(1)).notifyTransaction(Mockito.any(Message.class),Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(JsonProcessingException.class));
    }

    @Test
    void otherApplicationRetryTest(){
        // Given
        RewardTransactionDTO rtDT1 = RewardTransactionDTOFaker.mockInstance(1);
        RewardTransactionDTO rtDT2 = RewardTransactionDTOFaker.mockInstance(2);

        Flux<Message<String>> msgs = Flux.just(rtDT1, rtDT2)
                .map(TestUtils::jsonSerializer)
                .map(payload -> MessageBuilder
                        .withPayload(payload)
                        .setHeader(KafkaHeaders.RECEIVED_PARTITION, 0)
                        .setHeader(KafkaHeaders.OFFSET, 0L)
                )
                .doOnNext(m->m.setHeader(KafkaConstants.ERROR_MSG_HEADER_APPLICATION_NAME, "otherAppName".getBytes(StandardCharsets.UTF_8)))
                .map(MessageBuilder::build);

        // When
        persistenceTransactionMediator.execute(msgs);

        // Then
        Mockito.verifyNoInteractions(rewardTransactionMapper, rewardTransactionService, transactionErrorNotifierService);
    }
}