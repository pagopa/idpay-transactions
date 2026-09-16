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
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

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
        return SqlTransactionRetrySupport.retryOnConcurrencyFailure(Mono.defer(() -> {
            validateInput(sourceBatchId, initiativeId);
            return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                    .flatMap(connection -> reassignWithinTransaction(
                            org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                            sourceBatchId,
                            initiativeId
                    )));
        }));
    }

    private Mono<Void> reassignWithinTransaction(
            DSLContext transactionDslContext,
            String sourceBatchId,
            String initiativeId
    ) {
        return findBatch(transactionDslContext, sourceBatchId, initiativeId)
                .flatMap(source -> createOrReadTargetBatch(transactionDslContext, source)
                        .flatMap(target -> SqlRewardBatchRowLock.acquirePair(
                                        transactionDslContext,
                                        initiativeId,
                                        source.getId(),
                                        target.getId()
                                )
                                .flatMap(locked -> moveSuspendedTransactions(
                                        transactionDslContext,
                                        batchMapper.fromRecord(locked.source()),
                                        batchMapper.fromRecord(locked.target())
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

    private Mono<Void> moveSuspendedTransactions(
            DSLContext transactionDslContext,
            RewardBatch source,
            RewardBatch target
    ) {
        return SqlRewardBatchMembership.moveTransactions(
                        transactionDslContext,
                        source.getInitiativeId(),
                        source.getId(),
                        target.getId(),
                        REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                                RewardBatchTrxStatus.SUSPENDED.name()
                        ),
                        update -> update
                                .set(REWARD_TRANSACTIONS.STATUS, SyncTrxStatus.INVOICED.name())
                                .set(REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED,
                                        coalesce(
                                                REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED,
                                                val(source.getMonth())
                                        ))
                )
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
}
