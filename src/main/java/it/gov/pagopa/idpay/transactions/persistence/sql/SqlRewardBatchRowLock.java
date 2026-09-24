package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;

import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardBatchesRecord;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardTransactionsRecord;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jooq.DSLContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Coordinates SQL row locks used by reward-batch aggregate writers.
 *
 * <p>When a grouping advisory lock is used, it is acquired first. Batch rows
 * are then acquired in ascending ID order, followed by any transaction-row
 * lock or conditional membership update. Snapshot aggregate reads happen
 * after the batch-row lock and do not lock assigned transaction rows.</p>
 */
final class SqlRewardBatchRowLock {

    private SqlRewardBatchRowLock() {
    }

    static Mono<Map<String, RewardBatchesRecord>> acquire(
            DSLContext transactionDslContext,
            String initiativeId,
            Collection<String> batchIds
    ) {
        List<String> orderedBatchIds = batchIds.stream()
                .distinct()
                .sorted()
                .toList();
        if (orderedBatchIds.isEmpty()) {
            return Mono.error(new IllegalArgumentException("At least one reward batch ID is required"));
        }

        return Flux.fromIterable(orderedBatchIds)
                .concatMap(batchId -> Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(batchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate()))
                .collectMap(RewardBatchesRecord::getId);
    }

    static Mono<RewardBatchesRecord> acquireSingle(
            DSLContext transactionDslContext,
            String initiativeId,
            String batchId
    ) {
        return acquire(transactionDslContext, initiativeId, List.of(batchId))
                .flatMap(lockedBatches -> {
                    RewardBatchesRecord batch = lockedBatches.get(batchId);
                    return batch == null
                            ? Mono.error(new SqlMembershipChangedException(
                            "Reward batch %s changed while acquiring its row lock".formatted(batchId)
                    ))
                            : Mono.just(batch);
                });
    }

    static Mono<LockedBatchPair> acquirePair(
            DSLContext transactionDslContext,
            String initiativeId,
            String sourceBatchId,
            String targetBatchId
    ) {
        return acquire(transactionDslContext, initiativeId, List.of(sourceBatchId, targetBatchId))
                .flatMap(lockedBatches -> {
                    RewardBatchesRecord source = lockedBatches.get(sourceBatchId);
                    RewardBatchesRecord target = lockedBatches.get(targetBatchId);
                    return source == null || target == null
                            ? Mono.error(new SqlMembershipChangedException(
                            "Source or target reward batch changed while acquiring pair locks"
                    ))
                            : Mono.just(new LockedBatchPair(source, target));
                });
    }

    static Mono<RewardTransactionsRecord> acquireTransaction(
            DSLContext transactionDslContext,
            String transactionId,
            String expectedInitiativeId,
            String expectedBatchId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId))
                        .forUpdate())
                .switchIfEmpty(Mono.error(new SqlMembershipChangedException(
                        "Transaction %s disappeared while acquiring its row lock".formatted(transactionId)
                )))
                .flatMap(transaction -> Objects.equals(
                                expectedInitiativeId,
                                transaction.get(REWARD_TRANSACTIONS.INITIATIVE_ID)
                        ) && Objects.equals(
                                expectedBatchId,
                                transaction.get(REWARD_TRANSACTIONS.REWARD_BATCH_ID)
                        )
                        ? Mono.just(transaction)
                        : Mono.error(new SqlMembershipChangedException(
                                "Transaction %s changed membership while acquiring its row lock"
                                        .formatted(transactionId)
                        )));
    }

    record LockedBatchPair(
            RewardBatchesRecord source,
            RewardBatchesRecord target
    ) {
    }
}
