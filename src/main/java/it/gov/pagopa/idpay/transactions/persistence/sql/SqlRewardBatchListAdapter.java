package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchListPort;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SelectHavingStep;
import org.jooq.SortField;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.YearMonth;
import java.time.ZoneId;
import java.util.EnumSet;
import java.util.List;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.falseCondition;
import static org.jooq.impl.DSL.notExists;
import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.trueCondition;
import static org.jooq.impl.DSL.val;

/**
 * SQL read model for batch queues. Every returned batch includes counters derived
 * from its currently assigned transaction rows.
 */
@RequiredArgsConstructor
public class SqlRewardBatchListAdapter implements RewardBatchListPort {

    private static final Field<Long> NUMBER_OF_TRANSACTIONS = count().cast(Long.class)
            .as("number_of_transactions");
    private static final Field<Long> INITIAL_AMOUNT_CENTS = coalesce(
            sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS), val(0L)
    )
            .cast(Long.class)
            .as("initial_amount_cents");
    private static final Field<Long> NUMBER_OF_TRANSACTIONS_ELABORATED = count()
            .filterWhere(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(
                    RewardBatchTrxStatus.SUSPENDED.name(),
                    RewardBatchTrxStatus.APPROVED.name(),
                    RewardBatchTrxStatus.REJECTED.name()
            ))
            .cast(Long.class)
            .as("number_of_transactions_elaborated");
    private static final Field<Long> NUMBER_OF_TRANSACTIONS_SUSPENDED = count()
            .filterWhere(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(RewardBatchTrxStatus.SUSPENDED.name()))
            .cast(Long.class)
            .as("number_of_transactions_suspended");
    private static final Field<Long> NUMBER_OF_TRANSACTIONS_REJECTED = count()
            .filterWhere(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(RewardBatchTrxStatus.REJECTED.name()))
            .cast(Long.class)
            .as("number_of_transactions_rejected");
    private static final Field<Long> SUSPENDED_AMOUNT_CENTS = coalesce(
            sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS)
                    .filterWhere(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(RewardBatchTrxStatus.SUSPENDED.name())),
            val(0L)
    )
            .cast(Long.class)
            .as("suspended_amount_cents");
    private static final Field<Long> APPROVED_AMOUNT_CENTS_VALUE = coalesce(
            sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS)
                    .filterWhere(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(
                            RewardBatchTrxStatus.TO_CHECK.name(),
                            RewardBatchTrxStatus.CONSULTABLE.name(),
                            RewardBatchTrxStatus.APPROVED.name()
                    )),
            val(0L)
    )
                    .cast(Long.class);
    private static final Field<Long> APPROVED_AMOUNT_CENTS = APPROVED_AMOUNT_CENTS_VALUE
                    .as("approved_amount_cents");
    private static final ZoneId ROME_ZONE_ID = ZoneId.of("Europe/Rome");
    private static final RewardBatchSqlMapper.BatchAggregateProjection BATCH_AGGREGATE_PROJECTION =
                    new RewardBatchSqlMapper.BatchAggregateProjection(
                            NUMBER_OF_TRANSACTIONS,
                            INITIAL_AMOUNT_CENTS,
                            NUMBER_OF_TRANSACTIONS_ELABORATED,
                            NUMBER_OF_TRANSACTIONS_SUSPENDED,
                            NUMBER_OF_TRANSACTIONS_REJECTED,
                            SUSPENDED_AMOUNT_CENTS,
                            APPROVED_AMOUNT_CENTS
                    );

    private final DSLContext dslContext;
    private final RewardBatchSqlMapper mapper;

    @Override
    public Flux<RewardBatch> findRewardBatches(
            String merchantId,
            String initiativeId,
            String status,
            String assigneeLevel,
            String month,
            boolean isOperator,
            Pageable pageable
    ) {
        Pageable effectivePageable = effectivePageable(pageable);
        return Flux.from(projectedBatches(combinedCondition(
                        merchantId, initiativeId, status, assigneeLevel, month, isOperator
                ))
                .orderBy(sortFields(effectivePageable.getSort()))
                .limit(effectivePageable.getPageSize())
                .offset(effectivePageable.getOffset()))
                .map(this::toBatch);
    }

    @Override
    public Mono<Long> countRewardBatches(
            String merchantId,
            String initiativeId,
            String status,
            String assigneeLevel,
            String month,
            boolean isOperator
    ) {
        return Mono.from(dslContext.select(count())
                        .from(REWARD_BATCHES)
                        .where(combinedCondition(
                                merchantId, initiativeId, status, assigneeLevel, month, isOperator
                        )))
                .map(result -> result.value1().longValue());
    }

    @Override
    public Flux<RewardBatch> findBatchesBeforeMonth(
            String merchantId,
            String initiativeId,
            PosType posType,
            String month
    ) {
        return Flux.from(projectedBatches(REWARD_BATCHES.MERCHANT_ID.eq(merchantId)
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                        .and(REWARD_BATCHES.POS_TYPE.eq(posType.name()))
                        .and(REWARD_BATCHES.MONTH.lt(month)))
                .orderBy(REWARD_BATCHES.MONTH.asc()))
                .map(this::toBatch);
    }

    public Flux<RewardBatch> findBatchesWithStatus(
            RewardBatchStatus status,
            String initiativeId,
            Pageable pageable
    ) {
        Pageable effectivePageable = effectivePageable(pageable);
        return Flux.from(projectedBatches(REWARD_BATCHES.STATUS.eq(status.name())
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                .orderBy(sortFields(effectivePageable.getSort()))
                .limit(effectivePageable.getPageSize())
                .offset(effectivePageable.getOffset()))
                .map(this::toBatch);
    }

    public Flux<RewardBatch> findBatchesWithStatus(RewardBatchStatus status, String initiativeId) {
        return Flux.from(projectedBatches(REWARD_BATCHES.STATUS.eq(status.name())
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                .orderBy(REWARD_BATCHES.MONTH.asc()))
                .map(this::toBatch);
    }

    public Flux<RewardBatch> findDeliverableBatches(String initiativeId, Pageable pageable) {
        Pageable effectivePageable = effectivePageable(pageable);
        return Flux.from(projectedBatches(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.APPROVED.name())
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                .having(APPROVED_AMOUNT_CENTS_VALUE.gt(0L))
                .orderBy(sortFields(effectivePageable.getSort()))
                .limit(effectivePageable.getPageSize())
                .offset(effectivePageable.getOffset()))
                .map(this::toBatch);
    }

    public Flux<RewardBatch> findOutcomeBatches(String initiativeId, Pageable pageable) {
        return findBatchesWithStatus(RewardBatchStatus.PENDING_REFUND, initiativeId, pageable);
    }

    public Flux<RewardBatch> findEmptyBatchesBeforeCurrentMonth() {
        return Flux.from(projectedBatches(REWARD_BATCHES.MONTH.lt(YearMonth.now(ROME_ZONE_ID).toString())
                        .and(notExists(selectOne()
                                .from(REWARD_TRANSACTIONS)
                                .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(REWARD_BATCHES.ID)))))
                .orderBy(REWARD_BATCHES.MONTH.asc()))
                .map(this::toBatch);
    }

    private SelectHavingStep<Record> projectedBatches(Condition condition) {
        return dslContext.select(REWARD_BATCHES.fields())
                .select(
                        NUMBER_OF_TRANSACTIONS,
                        INITIAL_AMOUNT_CENTS,
                        NUMBER_OF_TRANSACTIONS_ELABORATED,
                        NUMBER_OF_TRANSACTIONS_SUSPENDED,
                        NUMBER_OF_TRANSACTIONS_REJECTED,
                        SUSPENDED_AMOUNT_CENTS,
                        APPROVED_AMOUNT_CENTS
                )
                .from(REWARD_BATCHES)
                .leftJoin(REWARD_TRANSACTIONS)
                .on(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(REWARD_BATCHES.ID)
                        .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(REWARD_BATCHES.INITIATIVE_ID)))
                .where(condition)
                .groupBy(REWARD_BATCHES.fields());
    }

    private RewardBatch toBatch(Record result) {
        return mapper.fromAggregateRecord(result, BATCH_AGGREGATE_PROJECTION);
    }

    private static Condition combinedCondition(
            String merchantId,
            String initiativeId,
            String status,
            String assigneeLevel,
            String month,
            boolean isOperator
    ) {
        Condition condition = trueCondition();
        if (notBlank(merchantId)) {
            condition = condition.and(REWARD_BATCHES.MERCHANT_ID.eq(merchantId));
        }
        if (notBlank(initiativeId)) {
            condition = condition.and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId));
        }
        if (notBlank(month)) {
            condition = condition.and(REWARD_BATCHES.MONTH.eq(month));
        }

        RewardBatchAssignee assignee = parseAssignee(assigneeLevel);
        if (assignee != null) {
            condition = condition.and(REWARD_BATCHES.ASSIGNEE_LEVEL.eq(assignee.name()));
        }

        return statusCondition(condition, status, assignee, isOperator);
    }

    private static Condition statusCondition(
            Condition condition,
            String requestedStatus,
            RewardBatchAssignee assignee,
            boolean isOperator
    ) {
        EnumSet<RewardBatchStatus> allowedStatuses = EnumSet.of(
                RewardBatchStatus.SENT,
                RewardBatchStatus.EVALUATING,
                RewardBatchStatus.APPROVING,
                RewardBatchStatus.APPROVED,
                RewardBatchStatus.TO_APPROVE,
                RewardBatchStatus.TO_WORK,
                RewardBatchStatus.PENDING_REFUND,
                RewardBatchStatus.REFUNDED,
                RewardBatchStatus.NOT_REFUNDED
        );
        if (!isOperator) {
            allowedStatuses.add(RewardBatchStatus.CREATED);
        }

        if (!notBlank(requestedStatus)) {
            return condition.and(REWARD_BATCHES.STATUS.in(allowedStatuses.stream()
                    .filter(status -> status != RewardBatchStatus.TO_APPROVE && status != RewardBatchStatus.TO_WORK)
                    .map(Enum::name)
                    .toList()));
        }

        RewardBatchStatus status = RewardBatchStatus.valueOf(requestedStatus);
        if (!allowedStatuses.contains(status)
                || (status == RewardBatchStatus.TO_APPROVE
                && assignee != null && assignee != RewardBatchAssignee.L3)
                || (status == RewardBatchStatus.TO_WORK && assignee == RewardBatchAssignee.L3)) {
            return condition.and(falseCondition());
        }

        return switch (status) {
            case TO_APPROVE -> condition.and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.EVALUATING.name()))
                    .and(assignee == null
                            ? REWARD_BATCHES.ASSIGNEE_LEVEL.eq(RewardBatchAssignee.L3.name())
                            : trueCondition());
            case TO_WORK -> condition.and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.EVALUATING.name()))
                    .and(assignee == null
                            ? REWARD_BATCHES.ASSIGNEE_LEVEL.in(
                                    RewardBatchAssignee.L1.name(), RewardBatchAssignee.L2.name()
                            )
                            : trueCondition());
            default -> condition.and(REWARD_BATCHES.STATUS.eq(status.name()));
        };
    }

    private static Pageable effectivePageable(Pageable pageable) {
        if (pageable == null || pageable.getSort().isUnsorted()) {
            return PageRequest.of(0, 10, Sort.by("month").ascending());
        }
        return pageable;
    }

    private static List<? extends SortField<?>> sortFields(Sort sort) {
        return sort.stream()
                .map(order -> sortableField(order.getProperty(), order.getDirection()))
                .toList();
    }

    private static SortField<?> sortableField(String property, Sort.Direction direction) {
        Field<?> field = switch (property) {
            case "id" -> REWARD_BATCHES.ID;
            case "merchantId" -> REWARD_BATCHES.MERCHANT_ID;
            case "initiativeId" -> REWARD_BATCHES.INITIATIVE_ID;
            case "businessName" -> REWARD_BATCHES.BUSINESS_NAME;
            case "month" -> REWARD_BATCHES.MONTH;
            case "posType" -> REWARD_BATCHES.POS_TYPE;
            case "status" -> REWARD_BATCHES.STATUS;
            case "partial" -> REWARD_BATCHES.PARTIAL;
            case "name" -> REWARD_BATCHES.NAME;
            case "startDate" -> REWARD_BATCHES.START_DATE;
            case "endDate" -> REWARD_BATCHES.END_DATE;
            case "assigneeLevel" -> REWARD_BATCHES.ASSIGNEE_LEVEL;
            case "creationDate" -> REWARD_BATCHES.CREATION_DATE;
            case "updateDate" -> REWARD_BATCHES.UPDATE_DATE;
            case "merchantSendDate" -> REWARD_BATCHES.MERCHANT_SEND_DATE;
            case "approvalDate" -> REWARD_BATCHES.APPROVAL_DATE;
            case "deliveryDateRequest" -> REWARD_BATCHES.DELIVERY_DATE_REQUEST;
            case "refundOutcomeTimestamp" -> REWARD_BATCHES.REFUND_OUTCOME_TIMESTAMP;
            case "reportPath" -> REWARD_BATCHES.REPORT_PATH;
            case "filename" -> REWARD_BATCHES.FILENAME;
            case "refundValutaDate" -> REWARD_BATCHES.REFUND_VALUTA_DATE;
            case "refundErrorMessage" -> REWARD_BATCHES.REFUND_ERROR_MESSAGE;
            case "numberOfTransactions" -> NUMBER_OF_TRANSACTIONS;
            case "initialAmountCents" -> INITIAL_AMOUNT_CENTS;
            case "numberOfTransactionsElaborated" -> NUMBER_OF_TRANSACTIONS_ELABORATED;
            case "numberOfTransactionsSuspended" -> NUMBER_OF_TRANSACTIONS_SUSPENDED;
            case "numberOfTransactionsRejected" -> NUMBER_OF_TRANSACTIONS_REJECTED;
            case "suspendedAmountCents" -> SUSPENDED_AMOUNT_CENTS;
            case "approvedAmountCents" -> APPROVED_AMOUNT_CENTS_VALUE;
            default -> REWARD_BATCHES.MONTH;
        };
        return direction.isAscending() ? field.asc() : field.desc();
    }

    private static RewardBatchAssignee parseAssignee(String assigneeLevel) {
        try {
            return notBlank(assigneeLevel) ? RewardBatchAssignee.valueOf(assigneeLevel) : null;
        } catch (IllegalArgumentException _) {
            return null;
        }
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}
