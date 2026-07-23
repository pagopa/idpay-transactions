package it.gov.pagopa.idpay.transactions.persistence.mongo;

import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionReadPort;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardTransactionReadAdapter implements RewardTransactionReadPort {

    private final RewardTransactionRepository rewardTransactionRepository;

    @Override
    public Mono<RewardTransaction> findById(String transactionId) {
        return rewardTransactionRepository.findById(transactionId);
    }
}
