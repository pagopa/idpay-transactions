package it.gov.pagopa.idpay.transactions.service;

import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

public interface SaveTransactionService{
    void execute(Flux<Message<String>> rewardTransactionDTOFlux);
}
