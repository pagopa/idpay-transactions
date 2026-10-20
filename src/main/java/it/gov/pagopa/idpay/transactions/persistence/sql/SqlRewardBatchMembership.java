package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;

import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardTransactionsRecord;
import java.util.function.UnaryOperator;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.UpdateSetMoreStep;
import org.jooq.impl.DSL;
import reactor.core.publisher.Mono;

/**
 * Applies conditional changes to the single current transaction membership.
 *
 * <p>Callers must acquire all affected batch rows through
 * {@link SqlRewardBatchRowLock} before invoking these methods. The conditional
 * source membership predicate is the retry boundary for concurrent moves.</p>
 */
final class SqlRewardBatchMembership {

    private SqlRewardBatchMembership() {
    }

    static Mono<RewardTransactionsRecord> moveTransaction(
            DSLContext transactionDslContext,
            String transactionId,
            String initiativeId,
            String sourceBatchId,
            String targetBatchId,
            UnaryOperator<UpdateSetMoreStep<RewardTransactionsRecord>> customize
    ) {
        return moveTransaction(
                transactionDslContext,
                transactionId,
                initiativeId,
                sourceBatchId,
                targetBatchId,
                DSL.noCondition(),
                customize
        );
    }

    static Mono<RewardTransactionsRecord> moveTransaction(
            DSLContext transactionDslContext,
            String transactionId,
            String initiativeId,
            String sourceBatchId,
            String targetBatchId,
            Condition additionalCondition,
            UnaryOperator<UpdateSetMoreStep<RewardTransactionsRecord>> customize
    ) {
        UpdateSetMoreStep<RewardTransactionsRecord> update = transactionDslContext
                .update(REWARD_TRANSACTIONS)
                .set(REWARD_TRANSACTIONS.REWARD_BATCH_ID, targetBatchId);

        return Mono.from(customize.apply(update)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(sourceBatchId))
                                .and(additionalCondition))
                        .returning())
                .switchIfEmpty(Mono.error(new SqlMembershipChangedException(
                        "Transaction %s changed membership while moving from batch %s"
                                .formatted(transactionId, sourceBatchId)
                )));
    }

    static Mono<Integer> moveTransactions(
            DSLContext transactionDslContext,
            String initiativeId,
            String sourceBatchId,
            String targetBatchId,
            Condition additionalCondition,
            UnaryOperator<UpdateSetMoreStep<RewardTransactionsRecord>> customize
    ) {
        UpdateSetMoreStep<RewardTransactionsRecord> update = transactionDslContext
                .update(REWARD_TRANSACTIONS)
                .set(REWARD_TRANSACTIONS.REWARD_BATCH_ID, targetBatchId);

        return Mono.from(customize.apply(update)
                .where(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId)
                        .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(sourceBatchId))
                        .and(additionalCondition)));
    }
}
