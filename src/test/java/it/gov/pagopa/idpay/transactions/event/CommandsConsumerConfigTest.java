package it.gov.pagopa.idpay.transactions.event;

import it.gov.pagopa.idpay.transactions.service.commands.CommandsMediatorService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

class CommandsConsumerConfigTest {
    @Test
    void consumerCommandsNotNull() {
        CommandsConsumerConfig commandsConsumer = new CommandsConsumerConfig();
        CommandsMediatorService commandsMediatorServiceMock = Mockito.mock(CommandsMediatorService.class);

        Consumer<Flux<Message<String>>> result = commandsConsumer.consumerCommands(commandsMediatorServiceMock);

        Assertions.assertNotNull(result);
    }

}