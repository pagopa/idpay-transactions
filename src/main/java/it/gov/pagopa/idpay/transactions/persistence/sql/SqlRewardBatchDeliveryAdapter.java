package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.currentLocalDateTime;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.val;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchDeliveryPort;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@RequiredArgsConstructor
@Component
public class SqlRewardBatchDeliveryAdapter implements RewardBatchDeliveryPort {

    private static final Field<Long> DERIVED_APPROVED_AMOUNT = coalesce(
            sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS)
                    .filterWhere(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(
                            RewardBatchTrxStatus.TO_CHECK.name(),
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            RewardBatchTrxStatus.APPROVED.name()
                    )),
            val(0L)
    ).cast(Long.class);

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final RewardBatchSqlMapper batchMapper;

    @Override
    public Mono<RewardBatch> snapshotDeliveryAmount(String rewardBatchId, String initiativeId) {
        return Mono.defer(() -> {
                    validateIdentity(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> snapshotWithinTransaction(
                                    org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    @Override
    public Mono<RewardBatch> recordDeliveryOutcome(
            String rewardBatchId,
            String initiativeId,
            DeliveryOutcomeDTO deliveryOutcome
    ) {
        if (deliveryOutcome == null) {
            return Mono.error(new IllegalArgumentException("Delivery outcome is required"));
        }
        return Mono.defer(() -> {
                    validateIdentity(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> recordDeliveryOutcomeWithinTransaction(
                                    org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId,
                                    deliveryOutcome
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    @Override
    public Mono<RewardBatch> recordRefundOutcome(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status,
            LocalDate refundValutaDate,
            String refundErrorMessage
    ) {
        validateRefundStatus(status);
        return Mono.defer(() -> {
                    validateIdentity(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> recordRefundOutcomeWithinTransaction(
                                    org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId,
                                    status,
                                    refundValutaDate,
                                    refundErrorMessage
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    private Mono<RewardBatch> snapshotWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.APPROVED.name())))
                        .forUpdate())
                .flatMap(batch -> {
                    if (batch.getDeliveryAmountCents() != null) {
                        return Mono.just(batchMapper.fromRecord(batch));
                    }

                    return Mono.from(transactionDslContext.select(DERIVED_APPROVED_AMOUNT)
                                    .from(REWARD_TRANSACTIONS)
                                    .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                            .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))))
                            .flatMap(amount -> amount.value1() > 0L
                                    ? Mono.from(transactionDslContext.update(REWARD_BATCHES)
                                                    .set(REWARD_BATCHES.DELIVERY_AMOUNT_CENTS, amount.value1())
                                                    .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                                                    .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                                            .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                                            .and(REWARD_BATCHES.STATUS.eq(
                                                                    RewardBatchStatus.APPROVED.name()
                                                            ))
                                                            .and(REWARD_BATCHES.DELIVERY_AMOUNT_CENTS.isNull()))
                                                    .returning())
                                            .map(batchMapper::fromRecord)
                                    : Mono.empty());
                });
    }

    private Mono<RewardBatch> recordDeliveryOutcomeWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            DeliveryOutcomeDTO deliveryOutcome
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .flatMap(batch -> {
                    if (deliveryOutcome.isSucceded()
                            && RewardBatchStatus.PENDING_REFUND.name().equals(batch.getStatus())) {
                        return Mono.just(batchMapper.fromRecord(batch));
                    }
                    if (!RewardBatchStatus.APPROVED.name().equals(batch.getStatus())) {
                        return Mono.empty();
                    }
                    if (batch.getDeliveryAmountCents() == null) {
                        return Mono.error(new IllegalStateException(
                                "Delivery amount was not snapshotted for batch %s".formatted(rewardBatchId)
                        ));
                    }

                    return deliveryOutcome.isSucceded()
                            ? markDeliveryAccepted(
                            transactionDslContext,
                            rewardBatchId,
                            initiativeId,
                            deliveryOutcome
                    )
                            : recordDeliveryRejection(
                            transactionDslContext,
                            rewardBatchId,
                            initiativeId,
                            deliveryOutcome
                    );
                });
    }

    private Mono<RewardBatch> markDeliveryAccepted(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            DeliveryOutcomeDTO deliveryOutcome
    ) {
        return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.DELIVERY_OUTCOME, batchMapper.toJooqJsonb(deliveryOutcome))
                        .set(REWARD_BATCHES.STATUS, RewardBatchStatus.PENDING_REFUND.name())
                        .set(REWARD_BATCHES.DELIVERY_DATE_REQUEST, currentLocalDateTime())
                        .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.APPROVED.name())))
                        .returning())
                .map(batchMapper::fromRecord);
    }

    private Mono<RewardBatch> recordDeliveryRejection(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            DeliveryOutcomeDTO deliveryOutcome
    ) {
        return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.DELIVERY_OUTCOME, batchMapper.toJooqJsonb(deliveryOutcome))
                        .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.APPROVED.name())))
                        .returning())
                .map(batchMapper::fromRecord);
    }

    private Mono<RewardBatch> recordRefundOutcomeWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status,
            LocalDate refundValutaDate,
            String refundErrorMessage
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .flatMap(batch -> {
                    if (status.name().equals(batch.getStatus())) {
                        return Mono.just(batchMapper.fromRecord(batch));
                    }
                    if (!RewardBatchStatus.PENDING_REFUND.name().equals(batch.getStatus())) {
                        return Mono.empty();
                    }

                    return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                                    .set(REWARD_BATCHES.STATUS, status.name())
                                    .set(REWARD_BATCHES.REFUND_VALUTA_DATE, refundValutaDate)
                                    .set(REWARD_BATCHES.REFUND_ERROR_MESSAGE, refundErrorMessage)
                                    .set(REWARD_BATCHES.REFUND_OUTCOME_TIMESTAMP, currentLocalDateTime())
                                    .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                                    .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                            .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                            .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.PENDING_REFUND.name())))
                                    .returning())
                            .map(batchMapper::fromRecord);
                });
    }

    private static void validateRefundStatus(RewardBatchStatus status) {
        if (status != RewardBatchStatus.REFUNDED && status != RewardBatchStatus.NOT_REFUNDED) {
            throw new IllegalArgumentException("Unsupported refund outcome status");
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
