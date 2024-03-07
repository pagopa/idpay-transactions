package it.gov.pagopa.idpay.transactions.service.commands;

import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.databind.ObjectReader;
import it.gov.pagopa.common.utils.MemoryAppender;
import it.gov.pagopa.common.utils.TestUtils;
import it.gov.pagopa.idpay.transactions.dto.QueueCommandOperationDTO;
import it.gov.pagopa.idpay.transactions.service.TransactionErrorNotifierServiceImpl;
import it.gov.pagopa.idpay.transactions.service.commands.ops.DeleteInitiativeService;
import it.gov.pagopa.idpay.transactions.utils.CommandsConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
class CommandsMediatorServiceImplTest {

    @Mock
    private TransactionErrorNotifierServiceImpl transactionErrorNotifierServiceMock;
    @Mock
    private DeleteInitiativeService deleteInitiativeServiceMock;
    @Mock
    private Message<String> messageMock;
    private CommandsMediatorServiceImpl commandMediatorService;
    private MemoryAppender memoryAppender;

    @BeforeEach
    void setUp() {
        commandMediatorService =
                new CommandsMediatorServiceImpl(
                        "Application Name",
                        100L,
                        deleteInitiativeServiceMock,
                        transactionErrorNotifierServiceMock,
                        TestUtils.objectMapper);

        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("it.gov.pagopa.idpay.transactions.service.commands.CommandsMediatorServiceImpl");
        memoryAppender = new MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.setLevel(ch.qos.logback.classic.Level.INFO);
        logger.addAppender(memoryAppender);
        memoryAppender.start();
    }

    @Test
    void getCommitDelay() {
        //given
        Duration expected = Duration.ofMillis(100L);
        //when
        Duration commitDelay = commandMediatorService.getCommitDelay();
        //then
        Assertions.assertEquals(expected,commitDelay);
    }

    @Test
    void givenMessagesWhenAfterCommitsThenSuccessfully() {
        //given
        Flux<List<String>> afterCommits2Subscribe = Flux.just(List.of("INITIATIVE1","INITIATIVE2","INITIATIVE3"));

        // when
        commandMediatorService.subscribeAfterCommits(afterCommits2Subscribe);

        //then
        Assertions.assertEquals(
                ("[TRANSACTIONS_COMMANDS] Processed offsets committed successfully"),
                memoryAppender.getLoggedEvents().get(0).getFormattedMessage()
        );
    }
    @Test
    void givenErrorWhenNotifyErrorThenCallNotifierService() {
        Throwable error = new RuntimeException("Test error");
        commandMediatorService.notifyError(messageMock, error);
        Mockito.verify(transactionErrorNotifierServiceMock).notifyTransactionCommands(
                messageMock,
                "[TRANSACTIONS_COMMANDS] An error occurred evaluating commands",
                true,
                error
        );
    }
    @Test
    void givenDeserializationErrorWhenOnDeserializationErrorThenCallNotifierService() {
        Throwable error = new RuntimeException("Test error");
        commandMediatorService.onDeserializationError(messageMock).accept(error);
        Mockito.verify(transactionErrorNotifierServiceMock).notifyTransactionCommands(
                messageMock,
                "[TRANSACTIONS_COMMANDS] Unexpected JSON",
                false,
                error
        );
    }
    @Test
    void getObjectReader() {
        ObjectReader objectReader = commandMediatorService.getObjectReader();
        Assertions.assertNotNull(objectReader);
    }

    @Test
    void givenDeleteInitiatveOperationTypeWhenCallExecuteThenReturnString() {
        //given
        QueueCommandOperationDTO payload = QueueCommandOperationDTO.builder()
                .entityId("DUMMY_INITITATIVEID")
                .operationTime(LocalDateTime.now())
                .operationType(CommandsConstants.COMMANDS_OPERATION_TYPE_DELETE_INITIATIVE)
                .build();

        Message<String> message = MessageBuilder.withPayload("INITIATIVE").setHeader("HEADER","DUMMY_HEADER").build();
        Map<String, Object> ctx = new HashMap<>();

        Mockito.when(deleteInitiativeServiceMock.execute(payload.getEntityId())).thenReturn(Mono.just(anyString()));

        //when
        String result = commandMediatorService.execute(payload, message, ctx).block();

        //then
        Assertions.assertNotNull(result);
        Mockito.verify(deleteInitiativeServiceMock).execute(anyString());
    }

    @Test
    void givenOperationTypeDifferentWhenCallExecuteThenReturnMonoEmpty(){
        //given
        QueueCommandOperationDTO payload = QueueCommandOperationDTO.builder()
                .entityId("DUMMY_INITITATIVEID")
                .operationTime(LocalDateTime.now())
                .operationType("OTHER_OPERATION_TYPE")
                .build();

        Message<String> message = MessageBuilder.withPayload("INITIATIVE").setHeader("HEADER","DUMMY_HEADER").build();
        Map<String, Object> ctx = new HashMap<>();
        //when
        Mono<String> result= commandMediatorService.execute(payload, message, ctx);

        //then
        assertEquals(result,Mono.empty());
        Mockito.verify(deleteInitiativeServiceMock,Mockito.never()).execute(anyString());
    }
    @Test
    void getFlowName() {
        //given
        String expected = "TRANSACTIONS_COMMANDS";
        //when
        String result = commandMediatorService.getFlowName();
        //then
        Assertions.assertEquals(expected,result);
    }
}
