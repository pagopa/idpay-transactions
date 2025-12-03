package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

public interface RewardTransactionService {
    Mono<RewardTransaction> save(RewardTransaction rewardTransaction);
    Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, Long amountCents, Pageable pageable);
    Mono<Void> assignInvoicedTransactionsToBatches(Integer chunkSize, Integer repetitionsNumber, boolean processAll, String trxId);
    Flux<RewardTransaction> findByInitiativeIdAndUserId(String initiativeId, String userId);
