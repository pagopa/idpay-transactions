package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;

import it.gov.pagopa.idpay.transactions.persistence.port.InitiativeTransactionDeletionPort;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class SqlInitiativeTransactionDeletionAdapter implements InitiativeTransactionDeletionPort {

    private final TransactionalOperator transactionalOperator;
    private final DSLContext dslContext;

    @Override
    public Mono<Long> deleteTransactions(String initiativeId) {
        return transactionalOperator.transactional(Mono.from(dslContext.deleteFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId)))
                .map(Integer::longValue));
    }
}
