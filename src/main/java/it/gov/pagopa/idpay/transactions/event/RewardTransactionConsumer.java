package it.gov.pagopa.idpay.transactions.event;

import it.gov.pagopa.idpay.transactions.service.PersistenceTransactionMediator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class RewardTransactionConsumer {

    @Bean
    public Consumer<Flux<Message<String>>> rewardTrxConsumer(PersistenceTransactionMediator persistenceTransactionMediator) {
        return persistenceTransactionMediator::execute;
    }
}
