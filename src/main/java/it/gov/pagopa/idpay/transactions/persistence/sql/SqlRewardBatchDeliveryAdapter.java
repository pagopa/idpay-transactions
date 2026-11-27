package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatchDeliveryEvents.REWARD_BATCH_DELIVERY_EVENTS;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatchDeliveryTransactions.REWARD_BATCH_DELIVERY_TRANSACTIONS;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.currentLocalDateTime;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.val;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.InvitaliaOutcomeStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchDeliveryPort;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Slf4j
@RequiredArgsConstructor
@Component
public class SqlRewardBatchDeliveryAdapter implements RewardBatchDeliveryPort {

    private static final String EVENT_TYPE_DELIVERY = "DELIVERY";
    private static final String EVENT_TYPE_OUTCOME = "OUTCOME";

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
                                    DSL.using(connection, SQLDialect.POSTGRES),
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
                                    DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId,
                                    deliveryOutcome
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    @Override
    public Mono<Void> recordDeliveryEvent(
            String rewardBatchId,
            String initiativeId,
            String outcome,
            Integer httpCode,
            JSONB requestJson,
            JSONB responseJson
    ) {
        return Mono.defer(() -> {
                    validateIdentity(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> insertEventWithinTransaction(
                                    DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId,
                                    EVENT_TYPE_DELIVERY,
                                    outcome,
                                    null,
                                    httpCode,
                                    null,
                                    requestJson,
                                    responseJson
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    @Override
    public Mono<Void> recordOutcomeEvent(
            String rewardBatchId,
            String initiativeId,
            String invitaliaStatus,
            Long amountPaidCents,
            JSONB responseJson
    ) {
        return Mono.defer(() -> {
                    validateIdentity(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> insertEventWithinTransaction(
                                    DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId,
                                    EVENT_TYPE_OUTCOME,
                                    null,
                                    invitaliaStatus,
                                    null,
                                    amountPaidCents,
                                    null,
                                    responseJson
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
                                    DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId,
                                    status,
                                    refundValutaDate,
                                    refundErrorMessage
                            )));
                })
                .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    @Override
    public Mono<RewardBatch> recordOutcomeCheck(
            String rewardBatchId,
            String initiativeId,
            OutcomeCheckCommand command,
            JSONB responseJson
    ) {
        if (command == null) {
            return Mono.error(new IllegalArgumentException("Outcome check command is required"));
        }
        return Mono.defer(() -> {
            validateIdentity(rewardBatchId, initiativeId);
            Mono<OutcomeCheckUpdateResult> batchUpdateMono = transactionalOperator.transactional(
                            ConnectionFactoryUtils.getConnection(connectionFactory)
                                    .flatMap(connection -> recordOutcomeCheckWithinTransaction(
                                            DSL.using(connection, SQLDialect.POSTGRES),
                                            rewardBatchId,
                                            initiativeId,
                                            command
                                    )))
                    .retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));

            return batchUpdateMono.flatMap(result -> {
                if (!result.statusChanged()) {
                    return Mono.just(result.batch());
                }
                boolean isCompletata = InvitaliaOutcomeStatus.COMPLETATA.name()
                        .equalsIgnoreCase(command.invitaliaStatus());
                Long eventAmountPaid = isCompletata ? command.amountPaidCents() : null;

                return recordOutcomeEvent(
                        rewardBatchId,
                        initiativeId,
                        command.invitaliaStatus(),
                        eventAmountPaid,
                        responseJson
                )
                        .onErrorResume(ex -> {
                            log.error("[GET_OUTCOME] Failed to persist outcome event for batch {}",
                                    rewardBatchId, ex);
                            return Mono.empty();
                        })
                        .thenReturn(result.batch());
            });
        });
    }

    private record OutcomeCheckUpdateResult(RewardBatch batch, boolean statusChanged) {
    }

    private Mono<OutcomeCheckUpdateResult> recordOutcomeCheckWithinTransaction(
            DSLContext ctx,
            String rewardBatchId,
            String initiativeId,
            OutcomeCheckCommand command
    ) {
        return Mono.from(ctx.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .flatMap(batch -> {
                    String invitaliaStatus = command.invitaliaStatus();
                    Long amountPaidCents = command.amountPaidCents();
                    String previousStatus = batch.getLastInvitaliaStatus();
                    boolean statusChanged = !Objects.equals(previousStatus, invitaliaStatus);
                    boolean isCompletata = InvitaliaOutcomeStatus.COMPLETATA.name().equalsIgnoreCase(invitaliaStatus);

                    if (isCompletata
                            && amountPaidCents != null
                            && batch.getDeliveryAmountCents() != null
                            && !Objects.equals(amountPaidCents, batch.getDeliveryAmountCents())) {
                        log.warn(
                                "[GET_OUTCOME] Paid amount mismatch for batch {}: paid={} cents, delivered={} cents",
                                rewardBatchId,
                                amountPaidCents,
                                batch.getDeliveryAmountCents()
                        );
                    }

                    String nextBatchStatus = batch.getStatus();
                    LocalDate nextValutaDate = batch.getRefundValutaDate();
                    String nextErrorMessage = batch.getRefundErrorMessage();
                    LocalDateTime nextOutcomeTimestamp = batch.getRefundOutcomeTimestamp();
                    boolean alreadyFinal = RewardBatchStatus.REFUNDED.name().equals(batch.getStatus())
                            || RewardBatchStatus.NOT_REFUNDED.name().equals(batch.getStatus());

                    if (!alreadyFinal && command.targetStatusIfAny() != null) {
                        if (RewardBatchStatus.PENDING_REFUND.name().equals(batch.getStatus())) {
                            nextBatchStatus = command.targetStatusIfAny().name();
                            nextValutaDate = command.refundValutaDate();
                            nextErrorMessage = command.refundErrorMessage();
                            nextOutcomeTimestamp = LocalDateTime.now();
                        } else {
                            return Mono.empty();
                        }
                    }

                    var updateQuery = ctx.update(REWARD_BATCHES)
                            .set(REWARD_BATCHES.LAST_OUTCOME_CHECK_DATE, currentLocalDateTime())
                            .set(REWARD_BATCHES.LAST_INVITALIA_STATUS, invitaliaStatus)
                            .set(REWARD_BATCHES.STATUS, nextBatchStatus)
                            .set(REWARD_BATCHES.REFUND_VALUTA_DATE, nextValutaDate)
                            .set(REWARD_BATCHES.REFUND_ERROR_MESSAGE, nextErrorMessage)
                            .set(REWARD_BATCHES.REFUND_OUTCOME_TIMESTAMP, nextOutcomeTimestamp)
                            .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime());

                    if (!alreadyFinal && isCompletata && amountPaidCents != null) {
                        updateQuery = updateQuery.set(REWARD_BATCHES.REFUND_AMOUNT_PAID_CENTS, amountPaidCents);
                    }

                    return Mono.from(updateQuery
                                    .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                            .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                                    .returning())
                            .map(batchMapper::fromRecord)
                            .map(updatedBatch -> new OutcomeCheckUpdateResult(updatedBatch, statusChanged));
                });
    }

    private Mono<Void> insertEventWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            String eventType,
            String outcome,
            String invitaliaStatus,
            Integer httpCode,
            Long amountPaidCents,
            JSONB requestJson,
            JSONB responseJson
    ) {
        return Mono.from(transactionDslContext
                        .select(coalesce(max(REWARD_BATCH_DELIVERY_EVENTS.ATTEMPT_NUMBER), val(0)).add(1))
                        .from(REWARD_BATCH_DELIVERY_EVENTS)
                        .where(REWARD_BATCH_DELIVERY_EVENTS.REWARD_BATCH_ID.eq(rewardBatchId)
                                .and(REWARD_BATCH_DELIVERY_EVENTS.EVENT_TYPE.eq(eventType))))
                .map(Record1::value1)
                .flatMap(nextAttempt -> Mono.from(transactionDslContext
                        .insertInto(REWARD_BATCH_DELIVERY_EVENTS)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.REWARD_BATCH_ID, rewardBatchId)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.INITIATIVE_ID, initiativeId)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.EVENT_TYPE, eventType)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.ATTEMPT_NUMBER, nextAttempt)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.OUTCOME, outcome)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.INVITALIA_STATUS, invitaliaStatus)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.HTTP_CODE, httpCode)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.AMOUNT_PAID_CENTS, amountPaidCents)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.REQUEST, requestJson)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.RESPONSE, responseJson)
                        .set(REWARD_BATCH_DELIVERY_EVENTS.CREATED_AT, currentLocalDateTime())
                        .onConflict(
                                REWARD_BATCH_DELIVERY_EVENTS.REWARD_BATCH_ID,
                                REWARD_BATCH_DELIVERY_EVENTS.EVENT_TYPE,
                                REWARD_BATCH_DELIVERY_EVENTS.ATTEMPT_NUMBER
                        )
                        .doNothing()))
                .then();
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
                            .flatMap(amount -> {
                                if (amount.value1() <= 0L) {
                                    return Mono.empty();
                                }

                                return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                                                .set(REWARD_BATCHES.DELIVERY_AMOUNT_CENTS, amount.value1())
                                                .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                                                .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                                        .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.APPROVED.name()))
                                                        .and(REWARD_BATCHES.DELIVERY_AMOUNT_CENTS.isNull()))
                                                .returning())
                                        .flatMap(updatedBatch -> insertDeliveryTransactionsSnapshot(
                                                transactionDslContext,
                                                rewardBatchId,
                                                initiativeId
                                        ).thenReturn(batchMapper.fromRecord(updatedBatch)));
                            });
                });
    }

    private Mono<Integer> insertDeliveryTransactionsSnapshot(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        var selectSnapshotRows = transactionDslContext
                .select(
                        REWARD_TRANSACTIONS.REWARD_BATCH_ID,
                        REWARD_TRANSACTIONS.TRANSACTION_ID,
                        REWARD_TRANSACTIONS.INITIATIVE_ID,
                        coalesce(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS, val(0L)).cast(Long.class),
                        REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                        currentLocalDateTime()
                )
                .from(REWARD_TRANSACTIONS)
                .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                        .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                        .and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(
                                RewardBatchTrxStatus.TO_CHECK.name(),
                                RewardBatchTrxStatus.CONSULTABLE.name(),
                                RewardBatchTrxStatus.APPROVED.name()
                        )));

        return Mono.from(transactionDslContext
                .insertInto(
                        REWARD_BATCH_DELIVERY_TRANSACTIONS,
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.REWARD_BATCH_ID,
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.TRANSACTION_ID,
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.INITIATIVE_ID,
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.ACCRUED_REWARD_CENTS,
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.CREATED_AT
                )
                .select(selectSnapshotRows)
                .onConflict(
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.REWARD_BATCH_ID,
                        REWARD_BATCH_DELIVERY_TRANSACTIONS.TRANSACTION_ID
                )
                .doNothing());
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
