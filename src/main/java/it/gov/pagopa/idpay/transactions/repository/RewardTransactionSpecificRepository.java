package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

public interface RewardTransactionSpecificRepository {
    Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Flux<RewardTransaction> findByFilter(String merchantId, String initiativeId, String userId, String status, Pageable pageable);
    Mono<Long> getCount(String merchantId, String initiativeId, String userId, String status);
    Mono<RewardTransaction> findOneByInitiativeId(String initiativeId);
    Mono<Void> removeInitiativeOnTransaction(String trxId, String initiativeId);
    Flux<RewardTransaction> findByInitiativesWithBatch(String initiativeId, int batchSize);
}
