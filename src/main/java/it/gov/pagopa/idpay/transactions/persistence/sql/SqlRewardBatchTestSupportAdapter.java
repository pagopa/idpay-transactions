package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_NOT_FOUND_BATCH;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.REWARD_BATCH_TEST_SUPPORT_NO_SAFE_MONTH;
import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.REWARD_BATCH_STATUS_MISMATCH;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.currentLocalDateTime;

import io.r2dbc.spi.ConnectionFactory;
import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.PreparedRewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardBatchFactory;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTestSupportPort;
import it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode;
import java.time.YearMonth;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record4;
import org.jooq.SQLDialect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.test-support.enabled", havingValue = "true")
public class SqlRewardBatchTestSupportAdapter implements RewardBatchTestSupportPort {

    private final TransactionalOperator transactionalOperator;
    private final ConnectionFactory connectionFactory;
    private final RewardBatchSqlMapper batchMapper;

    @Override
    public Mono<PreparedRewardBatch> prepareForSend(
            String initiativeId,
            String rewardBatchId,
            int searchHorizonMonths
    ) {
        if (searchHorizonMonths < 1) {
            return Mono.error(new IllegalArgumentException(
                    "Reward batch search horizon must be positive"
            ));
        }

        return transactionalOperator.transactional(
                        ConnectionFactoryUtils.getConnection(connectionFactory)
                                .flatMap(connection -> prepareWithinTransaction(
                                        org.jooq.impl.DSL.using(connection, SQLDialect.POSTGRES),
                                        initiativeId,
                                        rewardBatchId,
                                        searchHorizonMonths
                                ))
                );
    }

    private Mono<PreparedRewardBatch> prepareWithinTransaction(
            DSLContext transactionDslContext,
            String initiativeId,
            String rewardBatchId,
            int searchHorizonMonths
    ) {
        return lockSourceBatch(transactionDslContext, initiativeId, rewardBatchId)
                .flatMap(source -> validateCreated(source)
                        .then(readGroupingMonths(transactionDslContext, source))
                        .flatMap(months -> selectReferenceMonth(
                                        source,
                                        months,
                                        searchHorizonMonths
                                )
                                .map(referenceMonth -> updateReferenceMonth(
                                        transactionDslContext,
                                        source,
                                        referenceMonth
                                ))
                                .orElseGet(() -> noSafeMonth(rewardBatchId))));
    }

    private Mono<RewardBatch> lockSourceBatch(
            DSLContext transactionDslContext,
            String initiativeId,
            String rewardBatchId
    ) {
        return Mono.from(transactionDslContext.selectFrom(REWARD_BATCHES)
                        .where(REWARD_BATCHES.ID.eq(rewardBatchId)
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId)))
                        .forUpdate())
                .map(batchMapper::fromRecord)
                .switchIfEmpty(batchNotFound(rewardBatchId));
    }

    private <T> Mono<T> batchNotFound(String rewardBatchId) {
        return Mono.error(new ClientExceptionWithBody(
                HttpStatus.NOT_FOUND,
                ExceptionCode.REWARD_BATCH_NOT_FOUND,
                ERROR_MESSAGE_NOT_FOUND_BATCH.formatted(rewardBatchId)
        ));
    }

    private Mono<Void> validateCreated(RewardBatch source) {
        if (source.getStatus() == RewardBatchStatus.CREATED) {
            return Mono.empty();
        }
        return Mono.error(new ClientExceptionWithBody(
                HttpStatus.CONFLICT,
                ExceptionCode.REWARD_BATCH_STATUS_NOT_ALLOWED,
                REWARD_BATCH_STATUS_MISMATCH
        ));
    }

    private Mono<List<GroupingMonth>> readGroupingMonths(
            DSLContext transactionDslContext,
            RewardBatch source
    ) {
        return reactor.core.publisher.Flux.from(transactionDslContext
                        .select(
                                REWARD_BATCHES.ID,
                                REWARD_BATCHES.MONTH,
                                REWARD_BATCHES.STATUS,
                                count(REWARD_TRANSACTIONS.TRANSACTION_ID)
                        )
                        .from(REWARD_BATCHES)
                        .leftJoin(REWARD_TRANSACTIONS)
                        .on(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(REWARD_BATCHES.ID)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(
                                        REWARD_BATCHES.INITIATIVE_ID
                                )))
                        .where(REWARD_BATCHES.INITIATIVE_ID.eq(source.getInitiativeId())
                                .and(REWARD_BATCHES.MERCHANT_ID.eq(source.getMerchantId()))
                                .and(REWARD_BATCHES.POS_TYPE.eq(source.getPosType().name())))
                        .groupBy(
                                REWARD_BATCHES.ID,
                                REWARD_BATCHES.MONTH,
                                REWARD_BATCHES.STATUS
                        ))
                .map(this::toGroupingMonth)
                .collectList();
    }

    private GroupingMonth toGroupingMonth(Record4<String, String, String, Integer> row) {
        return new GroupingMonth(
                row.value1(),
                YearMonth.parse(row.value2()),
                RewardBatchStatus.valueOf(row.value3()),
                row.value4() > 0
        );
    }

    private Optional<YearMonth> selectReferenceMonth(
            RewardBatch source,
            List<GroupingMonth> groupingMonths,
            int searchHorizonMonths
    ) {
        YearMonth currentMonth = YearMonth.now(ZONEID);
        YearMonth sourceMonth = YearMonth.parse(source.getMonth());
        if (sourceMonth.isBefore(currentMonth)
                && hasNoEarlierCreatedBatch(source.getId(), sourceMonth, groupingMonths)) {
            return Optional.of(sourceMonth);
        }

        YearMonth candidate = currentMonth.minusMonths(1);
        for (int offset = 0; offset < searchHorizonMonths; offset++) {
            if (isAvailable(source.getId(), candidate, groupingMonths)
                    && hasNoEarlierCreatedBatch(source.getId(), candidate, groupingMonths)) {
                return Optional.of(candidate);
            }
            candidate = candidate.minusMonths(1);
        }
        return Optional.empty();
    }

    private boolean isAvailable(
            String sourceBatchId,
            YearMonth candidate,
            List<GroupingMonth> groupingMonths
    ) {
        return groupingMonths.stream()
                .noneMatch(month -> !month.batchId().equals(sourceBatchId)
                        && month.month().equals(candidate));
    }

    private boolean hasNoEarlierCreatedBatch(
            String sourceBatchId,
            YearMonth candidate,
            List<GroupingMonth> groupingMonths
    ) {
        return groupingMonths.stream()
                .noneMatch(month -> !month.batchId().equals(sourceBatchId)
                        && month.nonEmpty()
                        && month.status() == RewardBatchStatus.CREATED
                        && month.month().isBefore(candidate));
    }

    private Mono<PreparedRewardBatch> updateReferenceMonth(
            DSLContext transactionDslContext,
            RewardBatch source,
            YearMonth referenceMonth
    ) {
        String previousMonth = source.getMonth();
        if (referenceMonth.toString().equals(previousMonth)) {
            return Mono.just(new PreparedRewardBatch(
                    source.getId(),
                    previousMonth,
                    previousMonth,
                    source.getUpdateDate()
            ));
        }

        RewardBatch reference = RewardBatchFactory.create(
                source.getInitiativeId(),
                source.getMerchantId(),
                source.getPosType(),
                referenceMonth.toString(),
                source.getBusinessName()
        );

        return Mono.from(transactionDslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.MONTH, reference.getMonth())
                        .set(REWARD_BATCHES.START_DATE, reference.getStartDate())
                        .set(REWARD_BATCHES.END_DATE, reference.getEndDate())
                        .set(REWARD_BATCHES.NAME, reference.getName())
                        .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_BATCHES.ID.eq(source.getId())
                                .and(REWARD_BATCHES.INITIATIVE_ID.eq(source.getInitiativeId()))
                                .and(REWARD_BATCHES.STATUS.eq(RewardBatchStatus.CREATED.name()))
                                .and(REWARD_BATCHES.MONTH.eq(previousMonth)))
                        .returning())
                .map(updated -> new PreparedRewardBatch(
                        updated.get(REWARD_BATCHES.ID),
                        previousMonth,
                        updated.get(REWARD_BATCHES.MONTH),
                        updated.get(REWARD_BATCHES.UPDATE_DATE)
                ))
                .switchIfEmpty(Mono.error(new ClientExceptionWithBody(
                        HttpStatus.CONFLICT,
                        ExceptionCode.REWARD_BATCH_STATUS_NOT_ALLOWED,
                        REWARD_BATCH_STATUS_MISMATCH
                )));
    }

    private <T> Mono<T> noSafeMonth(String rewardBatchId) {
        return Mono.error(new ClientExceptionWithBody(
                HttpStatus.CONFLICT,
                ExceptionCode.REWARD_BATCH_TEST_SUPPORT_NO_SAFE_MONTH,
                "%s: %s".formatted(
                        REWARD_BATCH_TEST_SUPPORT_NO_SAFE_MONTH,
                        rewardBatchId
                )
        ));
    }

    private record GroupingMonth(
            String batchId,
            YearMonth month,
            RewardBatchStatus status,
            boolean nonEmpty
    ) {
    }
}
