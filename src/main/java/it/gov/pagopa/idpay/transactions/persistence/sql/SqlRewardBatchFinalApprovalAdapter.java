package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.currentLocalDateTime;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchFinalApprovalPort;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@RequiredArgsConstructor
@Component
public class SqlRewardBatchFinalApprovalAdapter implements RewardBatchFinalApprovalPort {

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final RewardBatchSqlMapper batchMapper;

    @Override
    public Mono<RewardBatch> prepareFinalApproval(String rewardBatchId, String initiativeId) {
        return Mono.defer(() -> {
                    validateIdentity(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> prepareWithinTransaction(
                                    org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    @Override
    public Mono<RewardBatch> completeFinalApproval(String rewardBatchId, String initiativeId) {
        return Mono.defer(() -> {
                    validateIdentity(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> completeWithinTransaction(
                                    org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    private Mono<RewardBatch> prepareWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.APPROVING.name()))
                                .and(REWARD_BATCHES.ASSIGNEE_LEVEL.eq(RewardBatchAssignee.L3.name())))
                        .forUpdate())
                .flatMap(batch -> Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                                .set(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                                        RewardBatchTrxStatus.APPROVED.name())
                                .set(REWARD_TRANSACTIONS.UPDATE_DATE, currentLocalDateTime())
                                .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                        .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                        .and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(
                                                RewardBatchTrxStatus.TO_CHECK.name(),
                                                RewardBatchTrxStatus.CONSULTABLE.name()
                                        ))))
                        .then(Mono.from(transactionDslContext.selectCount()
                                        .from(REWARD_TRANSACTIONS)
                                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                                                        RewardBatchTrxStatus.SUSPENDED.name()
                                                ))))
                                .map(count -> {
                                    RewardBatch result = batchMapper.fromRecord(batch);
                                    result.setNumberOfTransactionsSuspended(count.value1().longValue());
                                    return result;
                                })));
    }

    private Mono<RewardBatch> completeWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .flatMap(batch -> {
                    if (RewardBatchStatus.APPROVED.name().equals(batch.getStatus())) {
                        return Mono.just(batchMapper.fromRecord(batch));
                    }
                    if (!RewardBatchStatus.APPROVING.name().equals(batch.getStatus())
                            || !RewardBatchAssignee.L3.name().equals(batch.getAssigneeLevel())) {
                        return Mono.empty();
                    }

                    return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                                    .set(REWARD_BATCHES.STATUS, RewardBatchStatus.APPROVED.name())
                                    .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                                    .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                            .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                            .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.APPROVING.name()))
                                            .and(REWARD_BATCHES.ASSIGNEE_LEVEL.eq(RewardBatchAssignee.L3.name())))
                                    .returning())
                            .map(batchMapper::fromRecord);
                });
    }

    private static void validateIdentity(String rewardBatchId, String initiativeId) {
        if (isBlank(rewardBatchId) || isBlank(initiativeId)) {
            throw new IllegalArgumentException("Reward batch and initiative identifiers are required");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
