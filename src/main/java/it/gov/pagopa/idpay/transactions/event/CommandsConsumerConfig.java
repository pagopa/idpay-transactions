package it.gov.pagopa.idpay.transactions.event;

import it.gov.pagopa.idpay.transactions.service.commands.CommandsMediatorService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

@Configuration
public class CommandsConsumerConfig {

    @Bean
    public Consumer<Flux<Message<String>>> consumerCommands(CommandsMediatorService commandsMediatorService){
        return commandsMediatorService::execute;
    }
}
