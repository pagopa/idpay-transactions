package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.BATCH_CONTROL_THRESHOLD_NOT_MET;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_BATCH_CONTROL_THRESHOLD_NOT_MET;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static it.gov.pagopa.idpay.transactions.utils.RewardBatchControlThreshold.isSatisfied;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.currentLocalDateTime;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.common.web.exception.BatchControlThresholdNotMetException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchAssigneePromotionPort;
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
public class SqlRewardBatchAssigneePromotionAdapter implements RewardBatchAssigneePromotionPort {

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final DSLContext dslContext;
    private final RewardBatchSqlMapper batchMapper;

    @Override
    public Mono<RewardBatch> findBatchForPromotion(String rewardBatchId, String initiativeId) {
        validateIdentity(rewardBatchId, initiativeId);
        return Mono.from(dslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))))
                .map(batchMapper::fromRecord);
    }

    @Override
    public Mono<RewardBatch> promote(
            String rewardBatchId,
            String initiativeId,
            RewardBatchAssignee expectedAssignee,
            RewardBatchAssignee nextAssignee,
            int controlThresholdPercentage
    ) {
        validateIdentity(rewardBatchId, initiativeId);
        validateTransition(expectedAssignee, nextAssignee);
        return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                        .flatMap(connection -> promoteWithinTransaction(
                                org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                rewardBatchId,
                                initiativeId,
                                expectedAssignee,
                                nextAssignee,
                                controlThresholdPercentage
                        )))
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    private Mono<RewardBatch> promoteWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            RewardBatchAssignee expectedAssignee,
            RewardBatchAssignee nextAssignee,
            int controlThresholdPercentage
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.ASSIGNEE_LEVEL.eq(expectedAssignee.name())))
                        .forUpdate())
                .flatMap(ignored -> requiredElaboration(
                                transactionDslContext,
                                rewardBatchId,
                                initiativeId,
                                expectedAssignee,
                                controlThresholdPercentage
                        )
                        .then(Mono.from(transactionDslContext.update(REWARD_BATCHES)
                                        .set(REWARD_BATCHES.ASSIGNEE_LEVEL, nextAssignee.name())
                                        .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                                .and(REWARD_BATCHES.ASSIGNEE_LEVEL.eq(expectedAssignee.name())))
                                        .returning())
                                .map(batchMapper::fromRecord)));
    }

    private Mono<Void> requiredElaboration(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            RewardBatchAssignee expectedAssignee,
            int controlThresholdPercentage
    ) {
        return Mono.from(transactionDslContext.select(
                                count(),
                                count().filterWhere(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(
                                        RewardBatchTrxStatus.SUSPENDED.name(),
                                        RewardBatchTrxStatus.APPROVED.name(),
                                        RewardBatchTrxStatus.REJECTED.name()
                                ))
                        )
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))))
                .flatMap(counters -> {
                    long total = counters.value1().longValue();
                    long elaborated = counters.value2().longValue();
                    return !isSatisfied(total, elaborated, controlThresholdPercentage)
                            ? Mono.error(new BatchControlThresholdNotMetException(
                            BATCH_CONTROL_THRESHOLD_NOT_MET,
                            ERROR_MESSAGE_BATCH_CONTROL_THRESHOLD_NOT_MET.formatted(controlThresholdPercentage)
                    ))
                            : Mono.empty();
                });
    }

    private static void validateTransition(
            RewardBatchAssignee expectedAssignee,
            RewardBatchAssignee nextAssignee
    ) {
        boolean supported = (expectedAssignee == RewardBatchAssignee.L1
                && nextAssignee == RewardBatchAssignee.L2)
                || (expectedAssignee == RewardBatchAssignee.L2
                && nextAssignee == RewardBatchAssignee.L3);
        if (!supported) {
            throw new IllegalArgumentException("Unsupported reward batch assignee promotion");
        }
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
