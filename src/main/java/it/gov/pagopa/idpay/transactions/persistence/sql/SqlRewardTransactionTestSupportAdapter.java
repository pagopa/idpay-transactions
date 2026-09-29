package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionTestSupportPort;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.test-support.enabled", havingValue = "true")
public class SqlRewardTransactionTestSupportAdapter implements RewardTransactionTestSupportPort {

    private final TransactionalOperator transactionalOperator;
    private final RewardTransactionSqlRepository rewardTransactionSqlRepository;

    @Override
    public Flux<String> deleteByRewardBatchIdAndInitiativeIdReturningIds(String rewardBatchId, String initiativeId) {
        return transactionalOperator.transactional(rewardTransactionSqlRepository.deleteByRewardBatchIdAndInitiativeIdReturningIds(rewardBatchId, initiativeId));
    }
}
