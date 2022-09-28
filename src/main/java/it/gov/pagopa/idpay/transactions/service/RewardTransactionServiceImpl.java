package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Service
@Slf4j
public class RewardTransactionServiceImpl implements RewardTransactionService {

    private final RewardTransactionRepository rewardTrxRepository;


    public RewardTransactionServiceImpl(RewardTransactionRepository rewardTrxRepository) {
        this.rewardTrxRepository = rewardTrxRepository;
    }

    @Override
    public Mono<RewardTransaction> save(RewardTransaction rewardTransaction) {
        return rewardTrxRepository.save(rewardTransaction);
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(String idTrxIssuer, String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount, Pageable pageable) {
        return rewardTrxRepository.findByIdTrxIssuer(idTrxIssuer, userId, trxDateStart, trxDateEnd, amount, pageable);
    }

    @Override
    public Flux<RewardTransaction> findByRange(String userId, LocalDateTime trxDateStart, LocalDateTime trxDateEnd, BigDecimal amount, Pageable pageable) {
        return rewardTrxRepository.findByRange(userId, trxDateStart, trxDateEnd, amount, pageable);
    }


}
