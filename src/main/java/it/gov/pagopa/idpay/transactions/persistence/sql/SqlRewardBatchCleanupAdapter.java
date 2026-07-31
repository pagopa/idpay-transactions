package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.notExists;
import static org.jooq.impl.DSL.selectOne;

import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchEmptyCleanupPort;
import java.time.YearMonth;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SqlRewardBatchCleanupAdapter implements RewardBatchEmptyCleanupPort {

    private final TransactionalOperator transactionalOperator;
    private final DSLContext dslContext;

    @Override
    public Mono<Void> deleteEmptyBatches() {
        String currentMonth = YearMonth.now(ZONEID).toString();
        return transactionalOperator.transactional(Mono.from(dslContext.deleteFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.MONTH.lt(currentMonth)
                                .and(notExists(selectOne()
                                        .from(REWARD_TRANSACTIONS)
                                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(REWARD_BATCHES.ID)
                                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(
                                                        REWARD_BATCHES.INITIATIVE_ID)))))))
                .then());
    }
}
