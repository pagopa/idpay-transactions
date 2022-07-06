package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class RewardTransactionServiceImpl implements RewardTransactionService {

    private RewardTransactionRepository rewardTrxRepository;

    public RewardTransactionServiceImpl(RewardTransactionRepository rewardTrxRepository){
        this.rewardTrxRepository = rewardTrxRepository;
    }

    @Override
    public Mono<RewardTransaction> save(RewardTransaction transaction) {
        return rewardTrxRepository.save(transaction);
    }
}
