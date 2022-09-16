package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

public interface


RewardTransactionService {
    Flux<RewardTransaction> save(Flux<Message<String>> messageFlux);

   Flux<RewardTransaction> findAll(String startDate, String endDate, String userId, String hpan, String acquirerId);
}
