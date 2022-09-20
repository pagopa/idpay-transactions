package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public interface


RewardTransactionService {
    Flux<RewardTransaction> save(Flux<Message<String>> messageFlux);
    Flux<RewardTransaction> findTrxsFilters(String idTrxAcquirer, String userId, BigDecimal amount, LocalDateTime trxDateStart, LocalDateTime trxDateEnd);
}
