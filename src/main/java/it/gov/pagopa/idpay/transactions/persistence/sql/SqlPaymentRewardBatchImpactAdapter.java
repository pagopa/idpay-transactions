package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatchImpactInbox.REWARD_BATCH_IMPACT_INBOX;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcException;
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
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@RequiredArgsConstructor
public class SqlPaymentRewardBatchImpactAdapter implements PaymentRewardBatchImpactPort {

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final DSLContext dslContext;
    private final SqlRewardTransactionAdapter transactionAdapter;
    private final SqlRewardBatchAdapter batchAdapter;
    private final RewardTransactionSqlMapper transactionMapper;
    private final RewardBatchSqlMapper batchMapper;

    @Override
    public Mono<PaymentBatchEligibility> findEligibility(String merchantId, String transactionId) {
        return Mono.from(dslContext.select(
                        REWARD_TRANSACTIONS.TRANSACTION_ID,
                        REWARD_TRANSACTIONS.INITIATIVE_ID,
                        REWARD_TRANSACTIONS.MERCHANT_ID,
                        REWARD_TRANSACTIONS.REWARD_BATCH_ID,
                        REWARD_TRANSACTIONS.STATUS,
                        REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                        REWARD_BATCHES.STATUS
                )
                .from(REWARD_TRANSACTIONS)
                .join(REWARD_BATCHES)
                .on(REWARD_BATCHES.ID.eq(REWARD_TRANSACTIONS.REWARD_BATCH_ID)
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(REWARD_TRANSACTIONS.INITIATIVE_ID)))
                .where(REWARD_TRANSACTIONS.MERCHANT_ID.eq(merchantId)
                        .and(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId))))
                .map(record -> new PaymentBatchEligibility(
                        record.get(REWARD_TRANSACTIONS.TRANSACTION_ID),
                        record.get(REWARD_TRANSACTIONS.INITIATIVE_ID),
                        record.get(REWARD_TRANSACTIONS.MERCHANT_ID),
                        record.get(REWARD_TRANSACTIONS.REWARD_BATCH_ID),
                        record.get(REWARD_TRANSACTIONS.STATUS),
                        RewardBatchStatus.valueOf(record.get(REWARD_BATCHES.STATUS)),
                        rewardBatchTrxStatus(record.get(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS))
                ));
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
                                || isRetryableDatabaseConcurrencyFailure(error)));
    }

    private Mono<RewardTransaction> applyWithinTransaction(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact
    ) {
        return claimImpact(transactionDslContext, impact)
                .flatMap(claimed -> claimed
                        ? applyToCurrentMembership(transactionDslContext, impact)
                        : lockTransaction(transactionDslContext, impact.transaction().getId()));
    }

    private Mono<Boolean> claimImpact(DSLContext transactionDslContext, PaymentRewardBatchImpact impact) {
        return Mono.from(transactionDslContext.insertInto(REWARD_BATCH_IMPACT_INBOX)
                        .set(REWARD_BATCH_IMPACT_INBOX.EVENT_ID, impact.eventId())
                        .set(REWARD_BATCH_IMPACT_INBOX.TRANSACTION_ID, impact.transaction().getId())
                        .set(REWARD_BATCH_IMPACT_INBOX.TRANSACTION_REVISION, impact.transactionRevision())
                        .set(REWARD_BATCH_IMPACT_INBOX.IMPACT_TYPE, impact.impactType().name())
                        .onConflict(REWARD_BATCH_IMPACT_INBOX.EVENT_ID)
                        .doNothing()
                        .returning(REWARD_BATCH_IMPACT_INBOX.EVENT_ID))
                .map(ignored -> true)
                .switchIfEmpty(Mono.from(transactionDslContext.selectFrom(REWARD_BATCH_IMPACT_INBOX)
                                .where(REWARD_BATCH_IMPACT_INBOX.EVENT_ID.eq(impact.eventId())))
                        .flatMap(existing -> sameImpactIdentity(existing.getTransactionId(),
                                        existing.getTransactionRevision(),
                                        existing.getImpactType(),
                                        impact)
                                ? Mono.just(false)
                                : Mono.error(new IllegalStateException(
                                        "Payment reward batch impact event ID %s was reused"
                                                .formatted(impact.eventId())
                                )))
                        .switchIfEmpty(Mono.error(new IllegalStateException(
                                "Payment reward batch impact event %s could not be claimed"
                                        .formatted(impact.eventId())
                        ))));
    }

    private Mono<RewardTransaction> applyToCurrentMembership(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact
    ) {
        return findTransaction(transactionDslContext, impact.transaction().getId())
                .flatMap(existing -> existing.getRewardBatchId() == null
                        ? applyWithoutMembership(transactionDslContext, impact)
                        : applyWithCurrentMembership(transactionDslContext, impact, existing))
                .switchIfEmpty(applyWithoutMembership(transactionDslContext, impact));
    }

    private Mono<RewardTransaction> applyWithoutMembership(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact
    ) {
        return transactionAdapter.upsertImpactWithinTransaction(impact.transaction(), transactionDslContext)
                .flatMap(persisted -> {
                    if (persisted.getTransactionRevision() > impact.transactionRevision()) {
                        return Mono.just(persisted);
                    }
                    return lockTransaction(transactionDslContext, persisted.getId())
                            .flatMap(locked -> locked.getRewardBatchId() == null
                                    ? Mono.just(locked)
                                    : Mono.error(new MembershipChangedException()));
                });
    }

    private Mono<RewardTransaction> applyWithCurrentMembership(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact,
            RewardTransaction observed
    ) {
        String sourceBatchId = observed.getRewardBatchId();
        String sourceInitiativeId = observed.getInitiatives().getFirst();
        return lockBatch(transactionDslContext, sourceBatchId, sourceInitiativeId)
                .flatMap(source -> lockTransaction(transactionDslContext, observed.getId())
                        .flatMap(locked -> sourceBatchId.equals(locked.getRewardBatchId())
                                ? applyLockedMembership(
                                        transactionDslContext,
                                        impact,
                                        source,
                                        locked
                                )
                                : Mono.error(new MembershipChangedException())));
    }

    private Mono<RewardTransaction> applyLockedMembership(
            DSLContext transactionDslContext,
            PaymentRewardBatchImpact impact,
            RewardBatch source,
            RewardTransaction locked
    ) {
        if (!source.getMerchantId().equals(impact.transaction().getMerchantId())) {
            return Mono.error(new IllegalStateException(
                    "Transaction %s does not belong to source batch merchant %s"
                            .formatted(impact.transaction().getId(), source.getMerchantId())
            ));
        }
        return transactionAdapter.upsertImpactWithinTransaction(impact.transaction(), transactionDslContext)
                .flatMap(persisted -> {
                    if (persisted.getTransactionRevision() > impact.transactionRevision()) {
                        return Mono.just(persisted);
                    }
                    return switch (impact.impactType()) {
                        case INVOICE_REPLACED -> RewardBatchStatus.CREATED.equals(source.getStatus())
                                ? Mono.just(persisted)
                                : moveToOutcomeMonth(
                                        transactionDslContext,
                                        impact,
                                        source,
                                        persisted
                                );
                        case INVOICED_REVERSED -> detachMembership(
                                transactionDslContext,
                                source,
                                persisted
                        );
                    };
                });
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
                                        .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(source.getId()))
                                        .and(REWARD_TRANSACTIONS.TRANSACTION_REVISION.eq(
                                                impact.transactionRevision()
                                        )))
                                .returning())
                        .map(transactionMapper::fromRecord)
                        .switchIfEmpty(Mono.error(new MembershipChangedException())));
    }

    private Mono<RewardTransaction> detachMembership(
            DSLContext transactionDslContext,
            RewardBatch source,
            RewardTransaction transaction
    ) {
        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_ID, (String) null)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS, (String) null)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_INCLUSION_DATE, (java.time.LocalDateTime) null)
                        .set(REWARD_TRANSACTIONS.SAMPLING_KEY, 0)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transaction.getId())
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(source.getInitiativeId()))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(source.getId())))
                        .returning())
                .map(transactionMapper::fromRecord)
                .switchIfEmpty(Mono.error(new MembershipChangedException()));
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

    private Mono<RewardTransaction> lockTransaction(
            DSLContext transactionDslContext,
            String transactionId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId))
                        .forUpdate())
                .map(transactionMapper::fromRecord)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Transaction %s was not persisted".formatted(transactionId)
                )));
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
        if (impact.impactType() == PaymentRewardBatchImpactType.INVOICED_REVERSED
                && !SyncTrxStatus.REFUNDED.name().equals(impact.transaction().getStatus())) {
            throw new IllegalArgumentException("An invoiced reversal impact must be REFUNDED");
        }
        if (impact.impactType() == PaymentRewardBatchImpactType.INVOICE_REPLACED
                && impact.transaction().getPointOfSaleType() == null) {
            throw new IllegalArgumentException(
                    "An invoice replacement impact requires a point of sale type"
            );
        }
    }

    private static boolean sameImpactIdentity(
            String transactionId,
            long transactionRevision,
            String impactType,
            PaymentRewardBatchImpact impact
    ) {
        return transactionId.equals(impact.transaction().getId())
                && transactionRevision == impact.transactionRevision()
                && impactType.equals(impact.impactType().name());
    }

    private static boolean isRetryableDatabaseConcurrencyFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof R2dbcException exception
                    && ("40001".equals(exception.getSqlState())
                    || "40P01".equals(exception.getSqlState()))) {
                return true;
            }
        }
        return false;
    }

    private static final class MembershipChangedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
