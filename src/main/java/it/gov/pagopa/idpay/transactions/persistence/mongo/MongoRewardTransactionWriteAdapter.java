package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionWritePort;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardTransactionWriteAdapter implements RewardTransactionWritePort {

    private final RewardTransactionRepository rewardTransactionRepository;

    @Override
    public Mono<RewardTransaction> save(RewardTransaction rewardTransaction) {
        return rewardTransactionRepository.save(rewardTransaction);
    }

    @Override
    public Mono<Void> deleteById(String transactionId) {
        return rewardTransactionRepository.deleteById(transactionId);
    }
}
