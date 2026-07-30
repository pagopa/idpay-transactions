package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH;
import static org.jooq.impl.DSL.currentLocalDateTime;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoicedTransactionAssignmentPort;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SqlInvoicedTransactionAssignmentAdapter implements InvoicedTransactionAssignmentPort {

    private final TransactionalOperator transactionalOperator;
    private final DSLContext dslContext;
    private final ConnectionFactory connectionFactory;
    private final SqlRewardBatchAdapter batchAdapter;
    private final SqlRewardTransactionAdapter transactionAdapter;
    private final RewardBatchSqlMapper batchMapper;
    private final RewardTransactionSqlMapper transactionMapper;

    @Override
    public Flux<RewardTransaction> findInvoicedTransactionsWithoutBatch(int batchSize) {
        return Flux.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.STATUS.eq(SyncTrxStatus.INVOICED.name())
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.isNull()))
                        .orderBy(REWARD_TRANSACTIONS.TRANSACTION_ID.asc())
                        .limit(batchSize))
                .map(transactionMapper::fromRecord);
    }

    @Override
    public Mono<RewardTransaction> findInvoicedTransactionWithoutBatch(String transactionId) {
        return Mono.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)
                                .and(REWARD_TRANSACTIONS.STATUS.eq(SyncTrxStatus.INVOICED.name()))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.isNull())))
                .map(transactionMapper::fromRecord);
    }

    @Override
    public Mono<RewardTransaction> assignInvoicedTransaction(
            RewardTransaction transaction,
            RewardBatch batch,
            int samplingKey
    ) {
        String initiativeId = initiativeId(transaction);
        if (!initiativeId.equals(batch.getInitiativeId())) {
            return Mono.error(new IllegalArgumentException(
                    "Transaction and reward batch must belong to the same initiative"
            ));
        }

        return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                        .flatMap(connection -> assignWithinTransaction(
                                DSL.using(connection, SQLDialect.POSTGRES),
                                transaction,
                                batch,
                                samplingKey,
                                initiativeId
                        )))
                .onErrorMap(
                        BatchStatusMismatchException.class,
                        exception -> new ClientExceptionNoBody(HttpStatus.BAD_REQUEST, REWARD_BATCH_STATUS_MISMATCH)
                );
    }

    private Mono<RewardTransaction> assignWithinTransaction(
            DSLContext transactionDslContext,
            RewardTransaction transaction,
            RewardBatch batch,
            int samplingKey,
            String initiativeId
    ) {
        return transactionAdapter.upsertWithinTransaction(transaction, transactionDslContext)
                .flatMap(persisted -> SyncTrxStatus.INVOICED.name().equals(persisted.getStatus())
                        && persisted.getRewardBatchId() == null
                        ? lockOrCreateBatch(transactionDslContext, batch)
                                .flatMap(lockedBatch -> {
                                    if (lockedBatch.getStatus() != RewardBatchStatus.CREATED) {
                                        return Mono.error(new BatchStatusMismatchException());
                                    }
                                    return claimTransaction(
                                            transactionDslContext,
                                            persisted.getId(),
                                            initiativeId,
                                            lockedBatch.getId(),
                                            samplingKey
                                    );
                                })
                        : Mono.just(persisted));
    }

    private Mono<RewardBatch> lockOrCreateBatch(DSLContext transactionDslContext, RewardBatch batch) {
        if (batch.getId() == null) {
            batch.setId(UUID.randomUUID().toString());
        }

        return batchAdapter.createOrReadWithinTransaction(batch, transactionDslContext)
                .flatMap(created -> Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                                .where(REWARD_BATCHES.ID.eq(created.getId())
                                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(created.getInitiativeId())))
                                .forUpdate())
                        .map(batchMapper::fromRecord));
    }

    private Mono<RewardTransaction> claimTransaction(
            DSLContext transactionDslContext,
            String transactionId,
            String initiativeId,
            String rewardBatchId,
            int samplingKey
    ) {
        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_ID, rewardBatchId)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS,
                                RewardBatchTrxStatus.CONSULTABLE.name())
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_REJECTION_REASONS, (JSONB) null)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_INCLUSION_DATE, currentLocalDateTime())
                        .set(REWARD_TRANSACTIONS.SAMPLING_KEY, samplingKey)
                        .set(REWARD_TRANSACTIONS.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_TRANSACTIONS.STATUS.eq(SyncTrxStatus.INVOICED.name()))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.isNull()))
                        .returning())
                .map(transactionMapper::fromRecord)
                .switchIfEmpty(findPersistedTransaction(transactionDslContext, transactionId, initiativeId));
    }

    private Mono<RewardTransaction> findPersistedTransaction(
            DSLContext transactionDslContext,
            String transactionId,
            String initiativeId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))))
                .map(transactionMapper::fromRecord)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Transaction %s was not persisted".formatted(transactionId)
                )));
    }

    private static String initiativeId(RewardTransaction transaction) {
        List<String> initiatives = transaction.getInitiatives();
        if (initiatives == null || initiatives.size() != 1 || initiatives.getFirst().isBlank()) {
            throw new IllegalArgumentException("A reward transaction must have exactly one initiative");
        }
        return initiatives.getFirst();
    }

    private static final class BatchStatusMismatchException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
