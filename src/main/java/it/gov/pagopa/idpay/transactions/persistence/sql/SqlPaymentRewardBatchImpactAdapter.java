package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.idpay.transactions.enums.PaymentRewardBatchImpactType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.PaymentBatchEligibility;
import it.gov.pagopa.idpay.transactions.model.PaymentRewardBatchImpact;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.PaymentRewardBatchImpactPort;
import java.time.YearMonth;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
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
public class SqlPaymentRewardBatchImpactAdapter implements PaymentRewardBatchImpactPort {

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final DSLContext dslContext;
    private final SqlRewardTransactionAdapter transactionAdapter;
    private final SqlRewardBatchAdapter batchAdapter;
    private final RewardTransactionSqlMapper transactionMapper;
    private final RewardBatchSqlMapper batchMapper;

    @Override
    public Mono<PaymentBatchEligibility> findEligibility(String transactionId) {
        return Mono.from(dslContext.select(
                        REWARD_TRANSACTIONS.TRANSACTION_ID,
                        REWARD_TRANSACTIONS.INITIATIVE_ID,
                        REWARD_TRANSACTIONS.MERCHANT_ID,
                        REWARD_TRANSACTIONS.REWARD_BATCH_ID,
                        REWARD_TRANSACTIONS.STATUS,
                        REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                        REWARD_BATCHES.STATUS,
                        REWARD_BATCHES.MERCHANT_ID
                )
                .from(REWARD_TRANSACTIONS)
                .join(REWARD_BATCHES)
                .on(REWARD_BATCHES.ID.eq(REWARD_TRANSACTIONS.REWARD_BATCH_ID)
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(REWARD_TRANSACTIONS.INITIATIVE_ID)))
                .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)))
                .map(queryResult -> {
                    String transactionMerchantId = queryResult.get(REWARD_TRANSACTIONS.MERCHANT_ID);
                    String batchMerchantId = queryResult.get(REWARD_BATCHES.MERCHANT_ID);
                    if (!Objects.equals(transactionMerchantId, batchMerchantId)) {
                        throw new IllegalStateException(
                                "Reward batch merchant does not match transaction merchant");
                    }
                    return new PaymentBatchEligibility(
                            queryResult.get(REWARD_TRANSACTIONS.TRANSACTION_ID),
                            queryResult.get(REWARD_TRANSACTIONS.INITIATIVE_ID),
                            transactionMerchantId,
                            queryResult.get(REWARD_TRANSACTIONS.REWARD_BATCH_ID),
                            queryResult.get(REWARD_TRANSACTIONS.STATUS),
                            RewardBatchStatus.valueOf(queryResult.get(REWARD_BATCHES.STATUS)),
                            rewardBatchTrxStatus(
                                    queryResult.get(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS)
                            )
                    );
                });
    }

    @Override
    public Mono<RewardTransaction> applyImpact(PaymentRewardBatchImpact impact) {
        return Mono.defer(() -> {
                    validateImpact(impact);
                    impact.transaction().setTransactionRevision(impact.transactionRevision());
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> applyWithinTransaction(
                                    org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                    impact
                            )));
                })
                .retryWhen(Retry.max(3)
                        .filter(error -> error instanceof MembershipChangedException
                                || SqlTransactionRetrySupport.isRetryableConcurrencyFailure(error)));
    }

    private Mono<RewardTransaction> applyWithinTransaction(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact
    ) {
        return findTransaction(transactionDslContext, impact.transaction().getId())
                .flatMap(observed -> applyToObservedTransaction(
                        transactionDslContext,
                        impact,
                        observed
                ))
                .switchIfEmpty(insertAndApplyImpact(transactionDslContext, impact));
    }

    private Mono<RewardTransaction> applyToObservedTransaction(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact,
            RewardTransaction observed
    ) {
        if (observed.getRewardBatchId() != null) {
            return lockCurrentMembership(transactionDslContext, observed)
                    .flatMap(locked -> applyIfNewerImpact(
                            transactionDslContext,
                            impact,
                            locked.transaction(),
                            locked.source()
                    ));
        }
        return lockTransaction(transactionDslContext, observed.getId())
                .flatMap(locked -> {
                    if (locked.transaction().getRewardBatchId() != null) {
                        return Mono.error(new MembershipChangedException());
                    }
                    return applyIfNewerImpact(
                            transactionDslContext,
                            impact,
                            locked,
                            null
                    );
                });
    }

    private Mono<RewardTransaction> insertAndApplyImpact(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact
    ) {
        RewardTransactionEntity entity = transactionMapper.toEntity(impact.transaction());
        return Mono.from(transactionDslContext.insertInto(REWARD_TRANSACTIONS)
                        .set(transactionMapper.toRecord(entity))
                        .onConflict(REWARD_TRANSACTIONS.TRANSACTION_ID)
                        .doNothing()
                        .returning(REWARD_TRANSACTIONS.TRANSACTION_ID))
                .flatMap(ignored -> lockTransaction(transactionDslContext, entity.id())
                        .flatMap(locked -> applyIfNewerImpact(
                                transactionDslContext,
                                impact,
                                locked,
                                null
                        )))
                .switchIfEmpty(findTransaction(transactionDslContext, entity.id())
                        .flatMap(observed -> applyToObservedTransaction(
                                transactionDslContext,
                                impact,
                                observed
                        ))
                        .switchIfEmpty(Mono.error(new MembershipChangedException())));
    }

    private Mono<LockedMembership> lockCurrentMembership(
            DSLContext transactionDslContext,
            RewardTransaction observed
    ) {
        String sourceBatchId = observed.getRewardBatchId();
        String sourceInitiativeId = observed.getInitiatives().getFirst();
        return lockBatch(transactionDslContext, sourceBatchId, sourceInitiativeId)
                .flatMap(source -> lockTransaction(transactionDslContext, observed.getId())
                        .flatMap(locked -> sourceBatchId.equals(locked.transaction().getRewardBatchId())
                                ? Mono.just(new LockedMembership(locked, source))
                                : Mono.error(new MembershipChangedException())));
    }

    private Mono<RewardTransaction> applyIfNewerImpact(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact,
            LockedTransaction locked,
            RewardBatch source
    ) {
        if (!locked.transaction().getInitiatives().getFirst()
                .equals(impact.transaction().getInitiatives().getFirst())) {
            return Mono.error(new IllegalStateException(
                    "Transaction %s already belongs to initiative %s"
                            .formatted(
                                    impact.transaction().getId(),
                                    locked.transaction().getInitiatives().getFirst()
                            )
            ));
        }
        if (source != null && !source.getMerchantId().equals(impact.transaction().getMerchantId())) {
            return Mono.error(new IllegalStateException(
                    "Transaction %s does not belong to source batch merchant %s"
                            .formatted(impact.transaction().getId(), source.getMerchantId())
            ));
        }
        if (impact.transactionRevision() <= locked.latestAppliedPaymentImpactRevision()) {
            return Mono.just(locked.transaction());
        }
        return transactionAdapter.upsertImpactWithinTransaction(impact.transaction(), transactionDslContext)
                .flatMap(persisted -> applyLockedMembership(
                        transactionDslContext,
                        impact,
                        source,
                        persisted
                ))
                .flatMap(updated -> markImpactApplied(
                        transactionDslContext,
                        updated.getId(),
                        impact.transactionRevision()
                ).thenReturn(updated));
    }

    private Mono<RewardTransaction> applyLockedMembership(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact,
            RewardBatch source,
            RewardTransaction transaction
    ) {
        if (source == null) {
            return Mono.just(transaction);
        }
        return RewardBatchStatus.CREATED.equals(source.getStatus())
                ? Mono.just(transaction)
                : moveToOutcomeMonth(
                        transactionDslContext,
                        impact,
                        source,
                        transaction
                );
    }

    private Mono<RewardTransaction> moveToOutcomeMonth(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact,
            RewardBatch source,
            RewardTransaction transaction
    ) {
        return lockOrCreateOutcomeBatch(transactionDslContext, impact, source)
                .flatMap(target -> Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                                .set(REWARD_TRANSACTIONS.REWARD_BATCH_ID, target.getId())
                                .set(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                                        RewardBatchTrxStatus.SUSPENDED.name())
                                .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transaction.getId())
                                        .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(source.getInitiativeId()))
                                        .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(source.getId())))
                                .returning())
                        .map(transactionMapper::fromRecord)
                        .switchIfEmpty(Mono.error(new MembershipChangedException())));
    }

    private Mono<RewardBatch> lockOrCreateOutcomeBatch(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact,
            RewardBatch source
    ) {
        RewardTransaction transaction = impact.transaction();
        String outcomeMonth = YearMonth.from(impact.occurredAt().atZoneSameInstant(ZONEID)).toString();
        RewardBatch candidate = RewardBatchFactory.create(
                source.getInitiativeId(),
                source.getMerchantId(),
                transaction.getPointOfSaleType(),
                outcomeMonth,
                transaction.getBusinessName()
        );
        candidate.setId(UUID.randomUUID().toString());

        return batchAdapter.createOrReadWithinTransaction(candidate, transactionDslContext)
                .flatMap(target -> target.getId().equals(source.getId())
                        ? Mono.just(source)
                        : lockBatch(transactionDslContext, target.getId(), source.getInitiativeId()));
    }

    private Mono<RewardBatch> lockBatch(
            DSLContext transactionDslContext,
            String batchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(batchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .map(batchMapper::fromRecord)
                .switchIfEmpty(Mono.error(new MembershipChangedException()));
    }

    private Mono<RewardTransaction> findTransaction(
            DSLContext transactionDslContext,
            String transactionId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)))
                .map(transactionMapper::fromRecord);
    }

    private Mono<LockedTransaction> lockTransaction(
            DSLContext transactionDslContext,
            String transactionId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId))
                        .forUpdate())
                .map(transactionRecord -> new LockedTransaction(
                        transactionMapper.fromRecord(transactionRecord),
                        transactionRecord.getLatestAppliedPaymentImpactRevision()
                ))
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Transaction %s was not persisted".formatted(transactionId)
                )));
    }

    private Mono<Void> markImpactApplied(
            DSLContext transactionDslContext,
            String transactionId,
            long transactionRevision
    ) {
        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.LATEST_APPLIED_PAYMENT_IMPACT_REVISION,
                                transactionRevision)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)
                                .and(REWARD_TRANSACTIONS.LATEST_APPLIED_PAYMENT_IMPACT_REVISION
                                        .lt(transactionRevision)))
                        .returning(REWARD_TRANSACTIONS.TRANSACTION_ID))
                .switchIfEmpty(Mono.error(new MembershipChangedException()))
                .then();
    }

    private static RewardBatchTrxStatus rewardBatchTrxStatus(String value) {
        return value == null ? null : RewardBatchTrxStatus.valueOf(value);
    }

    private static void validateImpact(PaymentRewardBatchImpact impact) {
        if (impact == null
                || impact.eventId() == null
                || impact.eventId().isBlank()
                || impact.schemaVersion() < 1
                || impact.impactType() == null
                || impact.occurredAt() == null
                || impact.transactionRevision() < 1
                || impact.transaction() == null
                || impact.transaction().getId() == null
                || impact.transaction().getId().isBlank()
                || impact.transaction().getMerchantId() == null
                || impact.transaction().getMerchantId().isBlank()) {
            throw new IllegalArgumentException("Payment reward batch impact is incomplete");
        }
        if (impact.transaction().getTransactionRevision() != impact.transactionRevision()) {
            throw new IllegalArgumentException(
                    "Payment reward batch impact revisions do not match"
            );
        }
        if (impact.transaction().getRewardBatchId() != null
                || impact.transaction().getRewardBatchTrxStatus() != null
                    || impact.transaction().getRewardBatchRejectionReason() != null
                    || impact.transaction().getRewardBatchInclusionDate() != null
                    || impact.transaction().getRewardBatchLastMonthElaborated() != null
                    || impact.transaction().getSamplingKey() != 0
                    || impact.transaction().getChecksError() != null) {
            throw new IllegalArgumentException(
                    "Payment reward batch impact must not contain local batch membership"
            );
        }
        List<String> initiatives = impact.transaction().getInitiatives();
        if (initiatives == null || initiatives.size() != 1 || initiatives.getFirst().isBlank()) {
            throw new IllegalArgumentException(
                    "Payment reward batch impact transaction must have exactly one initiative"
            );
        }
        if (impact.impactType() == PaymentRewardBatchImpactType.INVOICE_REPLACED
                && !SyncTrxStatus.INVOICED.name().equals(impact.transaction().getStatus())) {
            throw new IllegalArgumentException("An invoice replacement impact must be INVOICED");
        }
        if (impact.impactType() == PaymentRewardBatchImpactType.INVOICE_REPLACED
                && impact.transaction().getPointOfSaleType() == null) {
            throw new IllegalArgumentException(
                    "An invoice replacement impact requires a point of sale type"
            );
        }
    }

    private static final class MembershipChangedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private record LockedTransaction(
            RewardTransaction transaction,
            long latestAppliedPaymentImpactRevision
    ) {
    }

    private record LockedMembership(
            LockedTransaction transaction,
            RewardBatch source
    ) {
    }
}
