package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_MONTH_TOO_EARLY;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_NOT_FOUND;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_PREVIOUS_NOT_SENT;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_STATE_BATCH;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_PREVIOUS_BATCH_TO_APPROVE;
import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.currentLocalDateTime;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.val;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardBatchesRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.Objects;

@RequiredArgsConstructor
@Component
public class SqlRewardBatchAdapter {

    private static final Field<Long> ASSIGNED_REWARD_AMOUNT_CENTS = coalesce(
            sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS),
            val(0L)
    ).cast(Long.class);
    private static final Field<Long> SUSPENDED_REWARD_AMOUNT_CENTS = coalesce(
            sum(REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS),
            val(0L)
    ).cast(Long.class);

    private final TransactionalOperator transactionalOperator;
    private final DSLContext dslContext;
    private final ConnectionFactory connectionFactory;
    private final RewardBatchSqlRepository repository;
    private final RewardBatchSqlMapper mapper;

    public Mono<RewardBatch> findById(String id) {
        return repository.findById(id).map(mapper::fromEntity);
    }

    public Mono<RewardBatch> findByIdAndInitiativeId(String id, String initiativeId) {
        return repository.findByIdAndInitiativeId(id, initiativeId).map(mapper::fromEntity);
    }

    public Mono<RewardBatch> findByMerchantInitiativeAndId(
            String merchantId,
            String initiativeId,
            String id
    ) {
        return repository.findByIdAndInitiativeIdAndMerchantId(id, initiativeId, merchantId)
                .map(mapper::fromEntity);
    }

    public Mono<RewardBatch> findByGrouping(
            String initiativeId,
            String merchantId,
            PosType posType,
            String month
    ) {
        return repository.findByInitiativeIdAndMerchantIdAndPosTypeAndMonth(
                        initiativeId,
                        merchantId,
                        posType.name(),
                        month
                )
                .map(mapper::fromEntity);
    }

    public Mono<RewardBatch> createOrRead(RewardBatch batch) {
        return transactionalOperator.transactional(createOrReadWithinTransaction(batch, dslContext));
    }

    Mono<RewardBatch> createOrReadWithinTransaction(RewardBatch batch, DSLContext transactionDslContext) {
        RewardBatchEntity entity = mapper.toEntity(batch);
        return insert(transactionDslContext, entity)
                .switchIfEmpty(findByGrouping(transactionDslContext,
                        entity.initiativeId(),
                        entity.merchantId(),
                        PosType.valueOf(entity.posType()),
                        entity.month()
                ));
    }

    public Mono<RewardBatch> save(RewardBatch batch) {
        RewardBatchEntity entity = mapper.toEntity(batch);
        return transactionalOperator.transactional(repository.existsById(entity.id())
                        .flatMap(exists -> exists.booleanValue()
                                ? repository.save(entity).map(mapper::fromEntity)
                                : insert(dslContext, entity).switchIfEmpty(findByGrouping(
                                        entity.initiativeId(),
                                        entity.merchantId(),
                                        PosType.valueOf(entity.posType()),
                                        entity.month()
                                ))));
    }

    public Mono<RewardBatch> sendBatch(
            String rewardBatchId,
            String initiativeId,
            String merchantId
    ) {
        return SqlTransactionRetrySupport.retryOnConcurrencyFailure(Mono.defer(() -> {
                    validateSendRequest(rewardBatchId, initiativeId);
                    return transactionalOperator.transactional(
                            ConnectionFactoryUtils.getConnection(connectionFactory)
                                    .flatMap(connection -> sendBatchWithinTransaction(
                                            DSL.using(connection, SQLDialect.POSTGRES),
                                            rewardBatchId,
                                            initiativeId,
                                            merchantId
                                    ))
                    );
                }));
    }

    public Mono<RewardBatch> enterApproval(String rewardBatchId, String initiativeId) {
        return SqlTransactionRetrySupport.retryOnConcurrencyFailure(Mono.defer(() -> {
            validateApprovalRequest(rewardBatchId, initiativeId);
            return transactionalOperator.transactional(
                    ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> enterApprovalWithinTransaction(
                                    DSL.using(connection, SQLDialect.POSTGRES),
                                    rewardBatchId,
                                    initiativeId
                            ))
            );
        }));
    }

    private Mono<RewardBatch> enterApprovalWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
    return lockBatchForApprovalWithGroupLock(transactionDslContext, rewardBatchId, initiativeId)
        .flatMap(
            batch -> {
              RewardBatchStatus status = RewardBatchStatus.valueOf(batch.getStatus());
              if (status != RewardBatchStatus.CREATED
                  && batch.getInitialAmountCentsAtSend() == null) {
                return Mono.error(missingSendSnapshot(batch.getId()));
              }
              if (status == RewardBatchStatus.APPROVING) {
                return returnExistingApproval(batch);
              }
              if (requiresApprovalSnapshot(status)
                  && batch.getSuspendedAmountCentsAtApproving() == null) {
                return Mono.error(missingApprovalSnapshot(batch.getId()));
              }
              if (status != RewardBatchStatus.EVALUATING) {
                return invalidApprovalState(batch.getId());
              }
              if (batch.getSuspendedAmountCentsAtApproving() != null) {
                return Mono.error(inconsistentApprovalSnapshot(batch.getId()));
              }
              if (!RewardBatchAssignee.L3.name().equals(batch.getAssigneeLevel())) {
                return invalidApprovalState(batch.getId());
              }

              return hasPreviousBatchToApprove(
                      transactionDslContext,
                      batch.getId(),
                      batch.getInitiativeId(),
                      batch.getMerchantId(),
                      batch.getPosType(),
                      batch.getMonth())
                  .flatMap(
                      hasPreviousBatch ->
                          Boolean.TRUE.equals(hasPreviousBatch)
                              ? Mono.error(
                                  new ClientExceptionWithBody(
                                      HttpStatus.BAD_REQUEST,
                                      REWARD_BATCH_INVALID_REQUEST,
                                      ERROR_MESSAGE_PREVIOUS_BATCH_TO_APPROVE.formatted(
                                          rewardBatchId)))
                              : captureApprovalSnapshot(
                                  transactionDslContext, rewardBatchId, initiativeId));
            });
    }

    private Mono<RewardBatchesRecord> lockBatchForApprovalWithGroupLock(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return readBatchForApproval(transactionDslContext, rewardBatchId, initiativeId)
                .flatMap(batch -> SqlRewardBatchGroupLock.acquire(
                                transactionDslContext,
                                batch.getInitiativeId(),
                                batch.getMerchantId(),
                                batch.getPosType()
                        )
                        .then(lockBatchForApproval(transactionDslContext, rewardBatchId, initiativeId)));
    }

    private Mono<RewardBatch> returnExistingApproval(RewardBatchesRecord batch) {
        if (batch.getSuspendedAmountCentsAtApproving() == null) {
            return Mono.error(missingApprovalSnapshot(batch.getId()));
        }
        if (!RewardBatchAssignee.L3.name().equals(batch.getAssigneeLevel())) {
            return invalidApprovalState(batch.getId());
        }
        return Mono.just(mapper.fromRecord(batch));
    }

    private Mono<RewardBatch> captureApprovalSnapshot(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return lockAssignedTransactions(transactionDslContext, rewardBatchId, initiativeId)
                .then(sumSuspendedRewards(transactionDslContext, rewardBatchId, initiativeId))
                .flatMap(amount -> captureApprovalSnapshotAndStatus(
                        transactionDslContext,
                        rewardBatchId,
                        initiativeId,
                        amount
                ));
    }

    private Mono<RewardBatchesRecord> lockBatchForApproval(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
                )));
    }

    private Mono<RewardBatchesRecord> readBatchForApproval(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
                )));
    }

    private Mono<Boolean> hasPreviousBatchToApprove(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            String merchantId,
            String posType,
            String month
    ) {
        return Mono.from(transactionDslContext.selectOne()
                        .from(REWARD_BATCHES)
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)
                                .and(REWARD_BATCHES.MERCHANT_ID.eq(merchantId))
                                .and(REWARD_BATCHES.POS_TYPE.eq(posType))
                                .and(REWARD_BATCHES.MONTH.lt(month))
                                .and(REWARD_BATCHES.ID.ne(rewardBatchId))
                                .and(REWARD_BATCHES.STATUS.notIn(
                                        RewardBatchStatus.APPROVED.name(),
                                        RewardBatchStatus.PENDING_REFUND.name(),
                                        RewardBatchStatus.NOT_REFUNDED.name(),
                                        RewardBatchStatus.REFUNDED.name()
                                )))
                        .limit(1))
                .hasElement();
    }

    private Mono<Long> sumSuspendedRewards(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.select(SUSPENDED_REWARD_AMOUNT_CENTS)
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                                        RewardBatchTrxStatus.SUSPENDED.name()
                                ))))
                .map(Record1::value1);
    }

    private Mono<RewardBatch> captureApprovalSnapshotAndStatus(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            Long amount
    ) {
        Field<LocalDateTime> now = currentLocalDateTime();
        return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.SUSPENDED_AMOUNT_CENTS_AT_APPROVING, amount)
                        .set(REWARD_BATCHES.STATUS, RewardBatchStatus.APPROVING.name())
                        .set(REWARD_BATCHES.APPROVAL_DATE, now)
                        .set(REWARD_BATCHES.UPDATE_DATE, now)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.EVALUATING.name()))
                                .and(REWARD_BATCHES.ASSIGNEE_LEVEL.eq(RewardBatchAssignee.L3.name()))
                                .and(REWARD_BATCHES.SUSPENDED_AMOUNT_CENTS_AT_APPROVING.isNull()))
                        .returning())
                .map(mapper::fromRecord)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Data integrity error: reward batch %s changed while capturing approval snapshot"
                                .formatted(rewardBatchId)
                )));
    }

    private Mono<RewardBatch> invalidApprovalState(String rewardBatchId) {
        return Mono.error(new ClientExceptionWithBody(
                HttpStatus.BAD_REQUEST,
                REWARD_BATCH_INVALID_REQUEST,
                ERROR_MESSAGE_INVALID_STATE_BATCH.formatted(rewardBatchId)
        ));
    }

    private static IllegalStateException missingApprovalSnapshot(String rewardBatchId) {
        return new IllegalStateException(
                "Data integrity error: reward batch %s is past EVALUATING without suspended approval snapshot"
                        .formatted(rewardBatchId)
        );
    }

    private static IllegalStateException inconsistentApprovalSnapshot(String rewardBatchId) {
        return new IllegalStateException(
                "Data integrity error: reward batch %s is EVALUATING with a suspended approval snapshot"
                        .formatted(rewardBatchId)
        );
    }

    public Mono<RewardBatch> updateMetadata(RewardBatch batch) {
        RewardBatchEntity entity = mapper.toEntity(batch);
        return transactionalOperator.transactional(Mono.from(dslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.BUSINESS_NAME, entity.businessName())
                        .set(REWARD_BATCHES.PARTIAL, entity.partial())
                        .set(REWARD_BATCHES.NAME, entity.name())
                        .set(REWARD_BATCHES.MERCHANT_SEND_DATE, entity.merchantSendDate())
                        .set(REWARD_BATCHES.APPROVAL_DATE, entity.approvalDate())
                        .set(REWARD_BATCHES.DELIVERY_DATE_REQUEST, entity.deliveryDateRequest())
                        .set(REWARD_BATCHES.DELIVERY_AMOUNT_CENTS, entity.deliveryAmountCents())
                        .set(REWARD_BATCHES.REFUND_OUTCOME_TIMESTAMP, entity.refundOutcomeTimestamp())
                        .set(REWARD_BATCHES.REPORT_PATH, entity.reportPath())
                        .set(REWARD_BATCHES.FILENAME, entity.filename())
                        .set(REWARD_BATCHES.ASSIGNEE_LEVEL, entity.assigneeLevel())
                        .set(REWARD_BATCHES.REFUND_VALUTA_DATE, entity.refundValutaDate())
                        .set(REWARD_BATCHES.REFUND_ERROR_MESSAGE, entity.refundErrorMessage())
                        .set(REWARD_BATCHES.DELIVERY_OUTCOME, jsonb(entity.deliveryOutcome()))
                        .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_BATCHES.ID.eq(entity.id()))
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(entity.initiativeId()))
                        .returning())
                .map(mapper::fromRecord));
    }

    private Mono<RewardBatch> sendBatchWithinTransaction(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            String merchantId
    ) {
        return lockBatchForSendWithGroupLock(transactionDslContext, rewardBatchId, initiativeId)
                .flatMap(batch -> processLockedBatch(
                        transactionDslContext,
                        batch,
                        rewardBatchId,
                        initiativeId,
                        merchantId
                ));
    }

    private Mono<RewardBatchesRecord> lockBatchForSendWithGroupLock(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return readBatchForSend(transactionDslContext, rewardBatchId, initiativeId)
                .flatMap(batch -> SqlRewardBatchGroupLock.acquire(
                                transactionDslContext,
                                batch.getInitiativeId(),
                                batch.getMerchantId(),
                                batch.getPosType()
                        )
                        .then(lockBatchForSend(transactionDslContext, rewardBatchId, initiativeId)));
    }

    private Mono<RewardBatch> processLockedBatch(
            DSLContext transactionDslContext,
            RewardBatchesRecord batch,
            String rewardBatchId,
            String initiativeId,
            String merchantId
    ) {
        if (!Objects.equals(merchantId, batch.getMerchantId())) {
            return Mono.error(new RewardBatchException(HttpStatus.NOT_FOUND, REWARD_BATCH_NOT_FOUND));
        }

        return switch (RewardBatchStatus.valueOf(batch.getStatus())) {
            case SENT -> returnSentBatch(batch);
            case CREATED -> sendCreatedBatch(
                    transactionDslContext,
                    batch,
                    rewardBatchId,
                    initiativeId
            );
            default -> rejectNonCreatedBatch(batch);
        };
    }

    private Mono<RewardBatch> returnSentBatch(RewardBatchesRecord batch) {
        return batch.getInitialAmountCentsAtSend() == null
                ? Mono.error(missingSendSnapshot(batch.getId()))
                : Mono.just(mapper.fromRecord(batch));
    }

    private Mono<RewardBatch> rejectNonCreatedBatch(RewardBatchesRecord batch) {
        return batch.getInitialAmountCentsAtSend() == null
                ? Mono.error(missingSendSnapshot(batch.getId()))
                : Mono.error(new RewardBatchException(HttpStatus.BAD_REQUEST, REWARD_BATCH_INVALID_REQUEST));
    }

    private Mono<RewardBatch> sendCreatedBatch(
            DSLContext transactionDslContext,
            RewardBatchesRecord batch,
            String rewardBatchId,
            String initiativeId
    ) {
        if (batch.getInitialAmountCentsAtSend() != null) {
            return Mono.error(inconsistentSendSnapshot(batch.getId()));
        }

        YearMonth batchMonth = YearMonth.parse(batch.getMonth());
        if (!YearMonth.now(ZONEID).isAfter(batchMonth)) {
            return Mono.error(new RewardBatchException(
                    HttpStatus.BAD_REQUEST,
                    REWARD_BATCH_MONTH_TOO_EARLY
            ));
        }

        return hasPreviousCreatedBatchWithTransactions(
                        transactionDslContext,
                        rewardBatchId,
                        initiativeId,
                        batch.getMerchantId(),
                        batch.getPosType(),
                        batchMonth
                )
                .flatMap(hasPreviousBatch -> hasPreviousBatch
                        ? Mono.error(new RewardBatchException(
                                HttpStatus.BAD_REQUEST,
                                REWARD_BATCH_PREVIOUS_NOT_SENT
                        ))
                        : captureBatchRewardSnapshot(
                                transactionDslContext,
                                rewardBatchId,
                                initiativeId
                        ));
    }

    private Mono<RewardBatch> captureBatchRewardSnapshot(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return lockAssignedTransactions(transactionDslContext, rewardBatchId, initiativeId)
                .then(sumAssignedRewards(transactionDslContext, rewardBatchId, initiativeId))
                .flatMap(amount -> captureSendSnapshotAndStatus(
                        transactionDslContext,
                        rewardBatchId,
                        initiativeId,
                        amount
                ));
    }

    private Mono<RewardBatchesRecord> lockBatchForSend(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .switchIfEmpty(Mono.error(new RewardBatchException(
                        HttpStatus.NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND
                )));
    }

    private Mono<RewardBatchesRecord> readBatchForSend(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))))
                .switchIfEmpty(Mono.error(new RewardBatchException(
                        HttpStatus.NOT_FOUND,
                        REWARD_BATCH_NOT_FOUND
                )));
    }

    private Mono<Boolean> hasPreviousCreatedBatchWithTransactions(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            String merchantId,
            String posType,
            YearMonth currentMonth
    ) {
        return Mono.from(transactionDslContext.selectOne()
                        .from(REWARD_BATCHES)
                        .join(REWARD_TRANSACTIONS)
                        .on(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(REWARD_BATCHES.ID)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(REWARD_BATCHES.INITIATIVE_ID)))
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)
                                .and(REWARD_BATCHES.MERCHANT_ID.eq(merchantId))
                                .and(REWARD_BATCHES.POS_TYPE.eq(posType))
                                .and(REWARD_BATCHES.MONTH.lt(currentMonth.toString()))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.CREATED.name()))
                                .and(REWARD_BATCHES.ID.ne(rewardBatchId)))
                        .limit(1))
                .hasElement();
    }

    private Mono<Void> lockAssignedTransactions(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Flux.from(transactionDslContext.select(REWARD_TRANSACTIONS.TRANSACTION_ID)
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .then();
    }

    private Mono<Long> sumAssignedRewards(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.select(ASSIGNED_REWARD_AMOUNT_CENTS)
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))))
                .map(Record1::value1);
    }

    private Mono<RewardBatch> captureSendSnapshotAndStatus(
            DSLContext transactionDslContext,
            String rewardBatchId,
            String initiativeId,
            Long amount
    ) {
        Field<LocalDateTime> now = currentLocalDateTime();
        return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.INITIAL_AMOUNT_CENTS_AT_SEND, amount)
                        .set(REWARD_BATCHES.STATUS, RewardBatchStatus.SENT.name())
                        .set(REWARD_BATCHES.MERCHANT_SEND_DATE, now)
                        .set(REWARD_BATCHES.UPDATE_DATE, now)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.CREATED.name()))
                                .and(REWARD_BATCHES.INITIAL_AMOUNT_CENTS_AT_SEND.isNull()))
                        .returning())
                .map(mapper::fromRecord)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Data integrity error: reward batch %s changed while capturing send snapshot"
                                .formatted(rewardBatchId)
                )));
    }

    private static IllegalStateException missingSendSnapshot(String rewardBatchId) {
        return new IllegalStateException(
                "Data integrity error: reward batch %s is SENT without initial send snapshot"
                        .formatted(rewardBatchId)
        );
    }

    private static IllegalStateException inconsistentSendSnapshot(String rewardBatchId) {
        return new IllegalStateException(
                "Data integrity error: reward batch %s is CREATED with an initial send snapshot"
                        .formatted(rewardBatchId)
        );
    }

    private static void validateSendRequest(String rewardBatchId, String initiativeId) {
        if (isBlank(rewardBatchId) || isBlank(initiativeId)) {
            throw new RewardBatchException(
                    HttpStatus.NOT_FOUND,
                    REWARD_BATCH_NOT_FOUND
            );
        }
    }

    private static void validateApprovalRequest(String rewardBatchId, String initiativeId) {
        if (isBlank(rewardBatchId) || isBlank(initiativeId)) {
            throw new ClientExceptionWithBody(
                    HttpStatus.NOT_FOUND,
                    REWARD_BATCH_NOT_FOUND,
                    ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
            );
        }
    }

    private static boolean requiresApprovalSnapshot(RewardBatchStatus status) {
        return switch (status) {
            case APPROVING, APPROVED, PENDING_REFUND, NOT_REFUNDED, REFUNDED -> true;
            default -> false;
        };
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private Mono<RewardBatch> findByGrouping(
            DSLContext transactionDslContext,
            String initiativeId,
            String merchantId,
            PosType posType,
            String month
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                        .and(REWARD_BATCHES.MERCHANT_ID.eq(merchantId))
                        .and(REWARD_BATCHES.POS_TYPE.eq(posType.name()))
                        .and(REWARD_BATCHES.MONTH.eq(month)))
                .map(mapper::fromRecord);
    }

    private Mono<RewardBatch> insert(DSLContext transactionDslContext, RewardBatchEntity entity) {
        return Mono.from(transactionDslContext.insertInto(REWARD_BATCHES)
                        .set(REWARD_BATCHES.ID, entity.id())
                        .set(REWARD_BATCHES.INITIATIVE_ID, entity.initiativeId())
                        .set(REWARD_BATCHES.MERCHANT_ID, entity.merchantId())
                        .set(REWARD_BATCHES.BUSINESS_NAME, entity.businessName())
                        .set(REWARD_BATCHES.MONTH, entity.month())
                        .set(REWARD_BATCHES.POS_TYPE, entity.posType())
                        .set(REWARD_BATCHES.STATUS, entity.status())
                        .set(REWARD_BATCHES.PARTIAL, entity.partial())
                        .set(REWARD_BATCHES.NAME, entity.name())
                        .set(REWARD_BATCHES.START_DATE, entity.startDate())
                        .set(REWARD_BATCHES.END_DATE, entity.endDate())
                        .set(REWARD_BATCHES.CREATION_DATE, entity.creationDate())
                        .set(REWARD_BATCHES.UPDATE_DATE, entity.updateDate())
                        .set(REWARD_BATCHES.MERCHANT_SEND_DATE, entity.merchantSendDate())
                        .set(REWARD_BATCHES.APPROVAL_DATE, entity.approvalDate())
                        .set(REWARD_BATCHES.DELIVERY_DATE_REQUEST, entity.deliveryDateRequest())
                        .set(REWARD_BATCHES.DELIVERY_AMOUNT_CENTS, entity.deliveryAmountCents())
                        .set(REWARD_BATCHES.REFUND_OUTCOME_TIMESTAMP, entity.refundOutcomeTimestamp())
                        .set(REWARD_BATCHES.REPORT_PATH, entity.reportPath())
                        .set(REWARD_BATCHES.FILENAME, entity.filename())
                        .set(REWARD_BATCHES.ASSIGNEE_LEVEL, entity.assigneeLevel())
                        .set(REWARD_BATCHES.REFUND_VALUTA_DATE, entity.refundValutaDate())
                        .set(REWARD_BATCHES.REFUND_ERROR_MESSAGE, entity.refundErrorMessage())
                        .set(REWARD_BATCHES.DELIVERY_OUTCOME, jsonb(entity.deliveryOutcome()))
                        .onConflict(
                                REWARD_BATCHES.INITIATIVE_ID,
                                REWARD_BATCHES.MERCHANT_ID,
                                REWARD_BATCHES.POS_TYPE,
                                REWARD_BATCHES.MONTH
                        )
                        .doNothing()
                        .returning())
                .map(mapper::fromRecord);
    }

    private static JSONB jsonb(io.r2dbc.postgresql.codec.Json value) {
        return value == null ? null : JSONB.jsonb(value.asString());
    }
}
