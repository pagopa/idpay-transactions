package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.TRANSACTION_NOT_FOUND;
import static org.jooq.impl.DSL.currentLocalDateTime;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.common.web.exception.ClientExceptionNoBody;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.MerchantTransactionPostponementPort;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Atomically moves a merchant-selected transaction to the next monthly batch.
 * Batch totals remain projections of the current transaction membership.
 */
@RequiredArgsConstructor
@Component
public class SqlMerchantTransactionPostponementAdapter implements MerchantTransactionPostponementPort {

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final SqlRewardBatchAdapter batchAdapter;
    private final RewardBatchSqlMapper batchMapper;
    private final RewardTransactionSqlMapper transactionMapper;

    @Override
    public Mono<RewardTransaction> postponeTransaction(
            String merchantId,
            String initiativeId,
            String sourceBatchId,
            String transactionId,
            LocalDate initiativeFruitionEndDate
    ) {
        return Mono.defer(() -> {
                    PostponementRequest request = new PostponementRequest(
                            merchantId,
                            initiativeId,
                            sourceBatchId,
                            transactionId,
                            initiativeFruitionEndDate
                    );
                    validateRequest(request);
                    return transactionalOperator.transactional(ConnectionFactoryUtils.getConnection(connectionFactory)
                            .flatMap(connection -> postponeWithinTransaction(
                                    org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                    request
                            )));
                })
                .retryWhen(Retry.max(3).filter(error -> error instanceof MembershipChangedException
                        || SqlTransactionRetrySupport.isRetryableConcurrencyFailure(error)));
    }

    private Mono<RewardTransaction> postponeWithinTransaction(
            DSLContext transactionDslContext,
            PostponementRequest request
    ) {
        return requireTransactionMembership(transactionDslContext, request)
                .then(lockSourceBatch(transactionDslContext, request))
                .flatMap(source -> lockTransaction(transactionDslContext, request)
                        .flatMap(transaction -> validateSourceBatch(source)
                                .then(validatePostponeLimit(source, request.initiativeFruitionEndDate()))
                                .then(lockOrCreateTargetBatch(transactionDslContext, source))
                                .flatMap(target -> validateTargetBatch(target)
                                        .then(moveTransaction(
                                                transactionDslContext,
                                                request,
                                                source,
                                                target
                                        )))));
    }

    private Mono<Void> requireTransactionMembership(
            DSLContext transactionDslContext,
            PostponementRequest request
    ) {
        return Mono.from(transactionDslContext.select(REWARD_TRANSACTIONS.TRANSACTION_ID)
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(request.transactionId())
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(request.initiativeId()))
                                .and(REWARD_TRANSACTIONS.MERCHANT_ID.eq(request.merchantId()))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(request.sourceBatchId()))))
                .switchIfEmpty(transactionNotFound(request.transactionId()))
                .then();
    }

    private Mono<RewardBatch> lockSourceBatch(
            DSLContext transactionDslContext,
            PostponementRequest request
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(request.sourceBatchId())
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(request.initiativeId())))
                        .forUpdate())
                .map(batchMapper::fromRecord)
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.NOT_FOUND,
                        ExceptionCode.REWARD_BATCH_NOT_FOUND,
                        ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(request.sourceBatchId())
                )))
                .flatMap(source -> source.getMerchantId().equals(request.merchantId())
                        ? Mono.just(source)
                        : transactionNotFound(request.transactionId()));
    }

    private Mono<RewardTransaction> lockTransaction(
            DSLContext transactionDslContext,
            PostponementRequest request
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(request.transactionId())
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(request.initiativeId()))
                                .and(REWARD_TRANSACTIONS.MERCHANT_ID.eq(request.merchantId()))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(request.sourceBatchId())))
                        .forUpdate())
                .map(transactionMapper::fromRecord)
                .switchIfEmpty(transactionNotFound(request.transactionId()));
    }

    private Mono<RewardBatch> lockOrCreateTargetBatch(
            DSLContext transactionDslContext,
            RewardBatch source
    ) {
        RewardBatch target = RewardBatchFactory.create(
                source.getInitiativeId(),
                source.getMerchantId(),
                source.getPosType(),
                nextMonth(source.getMonth()).toString(),
                source.getBusinessName()
        );
        target.setId(UUID.randomUUID().toString());

        return batchAdapter.createOrReadWithinTransaction(target, transactionDslContext)
                .flatMap(created -> Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                                .where(REWARD_BATCHES.ID.eq(created.getId())
                                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(source.getInitiativeId())))
                                .forUpdate())
                        .map(batchMapper::fromRecord)
                        .switchIfEmpty(Mono.error(new MembershipChangedException())));
    }

    private Mono<RewardTransaction> moveTransaction(
            DSLContext transactionDslContext,
            PostponementRequest request,
            RewardBatch source,
            RewardBatch target
    ) {
        return Mono.from(transactionDslContext.update(REWARD_TRANSACTIONS)
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_ID, target.getId())
                        .set(REWARD_TRANSACTIONS.REWARD_BATCH_INCLUSION_DATE, currentLocalDateTime())
                        .set(REWARD_TRANSACTIONS.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(request.transactionId())
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(request.initiativeId()))
                                .and(REWARD_TRANSACTIONS.MERCHANT_ID.eq(request.merchantId()))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(source.getId())))
                        .returning())
                .map(transactionMapper::fromRecord)
                .switchIfEmpty(Mono.error(new MembershipChangedException()));
    }

    private static Mono<RewardBatch> validateSourceBatch(RewardBatch source) {
        if (source.getStatus() != RewardBatchStatus.CREATED) {
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    ExceptionCode.REWARD_BATCH_INVALID_REQUEST,
                    REWARD_BATCH_STATUS_MISMATCH
            ));
        }
        return Mono.just(source);
    }

    private static Mono<RewardBatch> validateTargetBatch(RewardBatch target) {
        if (target.getStatus() != RewardBatchStatus.CREATED) {
            return Mono.error(new ClientExceptionNoBody(
                    HttpStatus.BAD_REQUEST,
                    REWARD_BATCH_STATUS_MISMATCH
            ));
        }
        return Mono.just(target);
    }

    private static Mono<Void> validatePostponeLimit(RewardBatch source, LocalDate initiativeFruitionEndDate) {
        YearMonth targetMonth = nextMonth(source.getMonth());
        YearMonth maxAllowedMonth = YearMonth.from(initiativeFruitionEndDate).plusMonths(1);
        if (targetMonth.isAfter(maxAllowedMonth)) {
            return Mono.error(new ClientExceptionWithBody(
                    HttpStatus.BAD_REQUEST,
                    ExceptionCode.REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED,
                    REWARD_BATCH_TRANSACTION_POSTPONE_LIMIT_EXCEEDED
            ));
        }
        return Mono.empty();
    }

    private static <T> Mono<T> transactionNotFound(String transactionId) {
        return Mono.error(new ClientExceptionNoBody(
                HttpStatus.NOT_FOUND,
                TRANSACTION_NOT_FOUND.formatted(transactionId)
        ));
    }

    private static YearMonth nextMonth(String sourceMonth) {
        return YearMonth.parse(sourceMonth).plusMonths(1);
    }

    private static void validateRequest(PostponementRequest request) {
        if (isBlank(request.merchantId())
                || isBlank(request.initiativeId())
                || isBlank(request.sourceBatchId())
                || isBlank(request.transactionId())
                || request.initiativeFruitionEndDate() == null) {
            throw new IllegalArgumentException(
                    "Merchant, initiative, source batch, transaction, and initiative fruition end date are required"
            );
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private record PostponementRequest(
            String merchantId,
            String initiativeId,
            String sourceBatchId,
            String transactionId,
            LocalDate initiativeFruitionEndDate
    ) {
    }

    private static final class MembershipChangedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
