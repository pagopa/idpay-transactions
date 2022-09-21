package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public interface RewardTransactionSpecificRepository {
    Flux<RewardTransaction> findByIdTrxIssuerAndOtherFilters(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount);
    Flux<RewardTransaction>  findByUserIdAndRangeDateAndAmount(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount);
}
