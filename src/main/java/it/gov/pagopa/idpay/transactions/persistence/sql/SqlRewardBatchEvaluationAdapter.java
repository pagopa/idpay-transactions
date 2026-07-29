package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.currentLocalDateTime;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionDecisionPort;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardBatchesRecord;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardTransactionsRecord;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/**
 * SQL mutation adapter for the evaluation phase. Batch counters stay derived
 * from transaction rows, so this adapter only changes lifecycle and row state.
 */
@RequiredArgsConstructor
public class SqlRewardBatchEvaluationAdapter implements RewardBatchTransactionDecisionPort {

    private static final long SAMPLE_PERCENT_NUMERATOR = 3L;
    private static final long SAMPLE_PERCENT_DENOMINATOR = 20L;

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final RewardBatchSqlMapper batchMapper;
    private final RewardTransactionSqlMapper transactionMapper;

    public Mono<RewardBatch> prepareEvaluation(String rewardBatchId, String initiativeId) {
        return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                .flatMap(connection -> prepareEvaluationWithinTransaction(
                        DSL.using(connection, SQLDialect.POSTGRES),
                        rewardBatchId,
                        initiativeId
                )));
    }

    @Override
    public Mono<RewardTransaction> updateStatusAndReturnOld(
            String initiativeId,
            String rewardBatchId,
            String transactionId,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason,
            String batchMonth,
            ChecksError checksError
    ) {
        return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                .flatMap(connection -> updateStatusWithinTransaction(
                        DSL.using(connection, SQLDialect.POSTGRES),
                        initiativeId,
                        rewardBatchId,
                        transactionId,
                        newStatus,
                        reason,
                        batchMonth,
                        checksError
                )));
    }

    private Mono<RewardBatch> prepareEvaluationWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return lockBatch(
                        transactionDslContext,
                        rewardBatchId,
                        initiativeId,
                        RewardBatchStatus.SENT
                )
                .flatMap(lockedBatch -> countBatchTransactions(
                                transactionDslContext,
                                rewardBatchId,
                                initiativeId
                        )
                        .flatMap(total -> markBatchTransactionsRewarded(
                                        transactionDslContext,
                                        rewardBatchId,
                                        initiativeId
                                )
                                .then(markSampleTransactionsForCheck(
                                        transactionDslContext,
                                        rewardBatchId,
                                        initiativeId,
                                        numberOfTransactionsToCheck(total)
                                ))
                                .then(moveBatchToEvaluating(
                                        transactionDslContext,
                                        rewardBatchId,
                                        initiativeId
                                ))));
    }

    private Mono<RewardTransaction> updateStatusWithinTransaction(
            DSLContext transactionDslContext,
            String initiativeId,
            String rewardBatchId,
            String transactionId,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason,
            String batchMonth,
            ChecksError checksError
    ) {
        return lockBatch(
                        transactionDslContext,
                        rewardBatchId,
                        initiativeId,
                        RewardBatchStatus.EVALUATING
                )
                .flatMap(ignored -> lockTransaction(
                        transactionDslContext,
                        initiativeId,
                        rewardBatchId,
                        transactionId
                ))
                .flatMap(current -> updateTransactionConditionally(
                        transactionDslContext,
                        initiativeId,
                        rewardBatchId,
                        newStatus,
                        reason,
                        batchMonth,
                        checksError,
                        current
                ));
    }

    private Mono<RewardBatchesRecord> lockBatch(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                        .and(REWARD_BATCHES.STATUS.eq(status.name())))
                .forUpdate());
    }

    private Mono<Long> countBatchTransactions(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectCount()
                        .from(REWARD_TRANSACTIONS)
                        .where(batchMembershipCondition(rewardBatchId, initiativeId)))
                .map(result -> result.value1().longValue());
    }

    private Mono<Void> markBatchTransactionsRewarded(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.STATUS, SyncTrxStatus.REWARDED.name())
                        .where(batchMembershipCondition(rewardBatchId, initiativeId)))
                .then();
    }

    private Mono<Void> markSampleTransactionsForCheck(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            int sampleSize
    ) {
        if (sampleSize == 0) {
            return Mono.empty();
        }

        RewardTransactions sampledTransactions = REWARD_TRANSACTIONS.as("sampled_transactions");
        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                                RewardBatchTrxStatus.TO_CHECK.name())
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.in(
                                transactionDslContext.select(sampledTransactions.TRANSACTION_ID)
                                        .from(sampledTransactions)
                                        .where(sampledTransactions.REWARD_BATCH_ID.eq(rewardBatchId)
                                                .and(sampledTransactions.INITIATIVE_ID.eq(initiativeId))
                                                .and(sampledTransactions.REWARD_BATCH_TRX_STATUS.isNull()
                                                        .or(sampledTransactions.REWARD_BATCH_TRX_STATUS.ne(
                                                                RewardBatchTrxStatus.SUSPENDED.name()
                                                        ))))
                                        .orderBy(
                                                sampledTransactions.SAMPLING_KEY.asc(),
                                                sampledTransactions.TRANSACTION_ID.asc()
                                        )
                                        .limit(sampleSize)
                        )))
                .then();
    }

    private Mono<RewardBatch> moveBatchToEvaluating(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.STATUS, RewardBatchStatus.EVALUATING.name())
                        .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.SENT.name())))
                        .returning())
                .map(batchMapper::fromRecord)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Locked reward batch %s could not move to evaluation".formatted(rewardBatchId)
                )));
    }

    private Mono<RewardTransactionsRecord> lockTransaction(
            DSLContext transactionDslContext,
            String initiativeId,
            String rewardBatchId,
            String transactionId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_TRANSACTIONS)
                .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)
                        .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId))
                        .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId)))
                .forUpdate());
    }

    private Mono<RewardTransaction> updateTransactionConditionally(
            DSLContext transactionDslContext,
            String initiativeId,
            String rewardBatchId,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason,
            String batchMonth,
            ChecksError checksError,
            RewardTransactionsRecord current
    ) {
        RewardTransaction oldTransaction = transactionMapper.fromRecord(current);
        JSONB rejectionReasons = transactionMapper.toJsonb(updatedRejectionReasons(
                oldTransaction,
                newStatus,
                reason
        ));
        JSONB checksErrorJson = transactionMapper.toJsonb(checksError);

        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS, newStatus.name())
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED, batchMonth)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_REJECTION_REASONS, rejectionReasons)
                        .set(REWARD_TRANSACTIONS.CHECKS_ERROR, checksErrorJson)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(current.getTransactionId())
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId))
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                .and(currentStatusCondition(current.getRewardBatchTrxStatus())))
                        .returning())
                .map(ignored -> oldTransaction);
    }

    private static Condition batchMembershipCondition(String rewardBatchId, String initiativeId) {
        return REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId));
    }

    private static Condition currentStatusCondition(String currentStatus) {
        return currentStatus == null
                ? REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.isNull()
                : REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(currentStatus);
    }

    private static List<ReasonDTO> updatedRejectionReasons(
            RewardTransaction oldTransaction,
            RewardBatchTrxStatus newStatus,
            ReasonDTO reason
    ) {
        if (reason == null) {
            return null;
        }
        if (oldTransaction.getRewardBatchTrxStatus() != newStatus) {
            return List.of(reason);
        }

        List<ReasonDTO> history = new ArrayList<>();
        if (oldTransaction.getRewardBatchRejectionReason() != null) {
            history.addAll(oldTransaction.getRewardBatchRejectionReason());
        }
        history.add(reason);
        return history;
    }

    private static int numberOfTransactionsToCheck(long total) {
        long completeGroups = total / SAMPLE_PERCENT_DENOMINATOR;
        long remainder = total % SAMPLE_PERCENT_DENOMINATOR;
        long sampleSize = completeGroups * SAMPLE_PERCENT_NUMERATOR
                + (remainder * SAMPLE_PERCENT_NUMERATOR + SAMPLE_PERCENT_DENOMINATOR - 1)
                / SAMPLE_PERCENT_DENOMINATOR;
        return Math.toIntExact(sampleSize);
    }
}
