package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.val;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.persistence.port.SuspendedTransactionReassignmentPort;
import java.time.YearMonth;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Moves final-approval suspended rows while batch aggregates remain derived
 * directly from the current transaction membership.
 */
@RequiredArgsConstructor
@Component
public class SqlSuspendedTransactionReassignmentAdapter implements SuspendedTransactionReassignmentPort {

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final SqlRewardBatchAdapter batchAdapter;
    private final RewardBatchSqlMapper batchMapper;

    @Override
    public Mono<Void> reassignSuspendedTransactions(String sourceBatchId, String initiativeId) {
        return Mono.defer(() -> {
            validateInput(sourceBatchId, initiativeId);
            return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                    .flatMap(connection -> reassignWithinTransaction(
                            org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                            sourceBatchId,
                            initiativeId
                    )));
        }).retryWhen(Retry.max(3).filter(SqlTransactionRetrySupport::isRetryableConcurrencyFailure));
    }

    private Mono<Void> reassignWithinTransaction(
            DSLContext transactionDslContext,
            String sourceBatchId,
            String initiativeId
    ) {
        return findBatch(transactionDslContext, sourceBatchId, initiativeId)
                .flatMap(source -> createOrReadTargetBatch(transactionDslContext, source)
                        .flatMap(target -> lockSourceAndTarget(
                                        transactionDslContext,
                                        source.getId(),
                                        target.getId(),
                                        initiativeId
                                )
                                .flatMap(locked -> moveSuspendedTransactions(
                                        transactionDslContext,
                                        locked
                                ))));
    }

    private Mono<RewardBatch> findBatch(
            DSLContext transactionDslContext,
            String batchId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(batchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))))
                .map(batchMapper::fromRecord)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Reward batch %s does not belong to initiative %s".formatted(batchId, initiativeId)
                )));
    }

    private Mono<RewardBatch> createOrReadTargetBatch(
            DSLContext transactionDslContext,
            RewardBatch source
    ) {
        RewardBatch target = RewardBatchFactory.create(
                source.getInitiativeId(),
                source.getMerchantId(),
                source.getPosType(),
                targetMonth(source.getMonth()),
                source.getBusinessName()
        );
        target.setId(UUID.randomUUID().toString());
        return batchAdapter.createOrReadWithinTransaction(target, transactionDslContext);
    }

    private Mono<LockedBatches> lockSourceAndTarget(
            DSLContext transactionDslContext,
            String sourceBatchId,
            String targetBatchId,
            String initiativeId
    ) {
        List<String> batchIds = List.of(sourceBatchId, targetBatchId).stream()
                .distinct()
                .sorted()
                .toList();

        return Flux.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.in(batchIds)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .orderBy(REWARD_BATCHES.ID.asc())
                        .forUpdate())
                .map(batchMapper::fromRecord)
                .collectMap(RewardBatch::getId)
                .flatMap(lockedBatches -> lockedBatches.containsKey(sourceBatchId)
                                && lockedBatches.containsKey(targetBatchId)
                        ? Mono.just(new LockedBatches(
                                lockedBatches.get(sourceBatchId),
                                lockedBatches.get(targetBatchId)
                        ))
                        : Mono.error(new IllegalStateException(
                                "Source or target reward batch changed during suspended reassignment"
                        )));
    }

    private Mono<Void> moveSuspendedTransactions(
            DSLContext transactionDslContext,
            LockedBatches lockedBatches
    ) {
        RewardBatch source = lockedBatches.source();
        RewardBatch target = lockedBatches.target();

        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_ID, target.getId())
                        .set(REWARD_TRANSACTIONS.STATUS, SyncTrxStatus.INVOICED.name())
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED,
                                coalesce(
                                        REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED,
                                        val(source.getMonth())
                                ))
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(source.getId())
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(source.getInitiativeId()))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                                        RewardBatchTrxStatus.SUSPENDED.name()
                                ))))
                .then();
    }

    private static String targetMonth(String sourceMonth) {
        YearMonth source = YearMonth.parse(sourceMonth);
        YearMonth current = YearMonth.now(ZONEID);
        return source.isAfter(current) ? source.toString() : current.toString();
    }

    private static void validateInput(String sourceBatchId, String initiativeId) {
        if (sourceBatchId == null || sourceBatchId.isBlank()
                || initiativeId == null || initiativeId.isBlank()) {
            throw new IllegalArgumentException("Source batch ID and initiative ID are required");
        }
    }

    private record LockedBatches(RewardBatch source, RewardBatch target) {
    }
}
