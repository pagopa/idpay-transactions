package it.gov.pagopa.idpay.transactions.event;

import it.gov.pagopa.idpay.transactions.service.PersistenceTransactionMediator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

class RewardTransactionConsumerTest {
    @Test
    void rewardTransactionConsumerNotNull() {
        RewardTransactionConsumer rewardTransactionConsumer = new RewardTransactionConsumer();
        PersistenceTransactionMediator persistenceTransactionMediatorMock = Mockito.mock(PersistenceTransactionMediator.class);

        Consumer<Flux<Message<String>>> result = rewardTransactionConsumer.rewardTrxConsumer(persistenceTransactionMediatorMock);

        Assertions.assertNotNull(result);
    }
}