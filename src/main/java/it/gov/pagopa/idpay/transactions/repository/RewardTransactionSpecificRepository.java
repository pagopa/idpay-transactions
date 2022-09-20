package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public interface RewardTransactionSpecificRepository {
    Flux<RewardTransaction> findByFilters(String idTrxIssuer, String userId, BigDecimal amount, LocalDateTime trxDateStart, LocalDateTime trxDateEnd);
}
