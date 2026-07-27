package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardBatches.REWARD_BATCHES;
import static org.jooq.impl.DSL.currentLocalDateTime;

@RequiredArgsConstructor
public class SqlRewardBatchAdapter {

    private final TransactionalOperator transactionalOperator;
    private final DSLContext dslContext;
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
        RewardBatchEntity entity = mapper.toEntity(batch, false);
        return transactionalOperator.transactional(insert(entity)
                .switchIfEmpty(findByGrouping(
                        entity.initiativeId(),
                        entity.merchantId(),
                        PosType.valueOf(entity.posType()),
                        entity.month()
                )));
    }

    public Mono<RewardBatch> save(RewardBatch batch) {
        return transactionalOperator.transactional(repository.existsById(batch.getId())
                        .flatMap(exists -> repository.save(mapper.toEntity(batch, !exists))))
                .map(mapper::fromEntity);
    }

    public Mono<RewardBatch> updateStatus(
            String id,
            String initiativeId,
            RewardBatchStatus status
    ) {
        return transactionalOperator.transactional(Mono.from(dslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.STATUS, status.name())
                        .set(REWARD_BATCHES.UPDATE_DATE, currentLocalDateTime())
                        .where(REWARD_BATCHES.ID.eq(id))
                        .and(REWARD_BATCHES.INITIATIVE_ID.eq(initiativeId))
                        .returning())
                .map(mapper::fromRecord));
    }

    public Mono<RewardBatch> updateMetadata(RewardBatch batch) {
        RewardBatchEntity entity = mapper.toEntity(batch, false);
        return transactionalOperator.transactional(Mono.from(dslContext.update(REWARD_BATCHES)
                        .set(REWARD_BATCHES.BUSINESS_NAME, entity.businessName())
                        .set(REWARD_BATCHES.PARTIAL, entity.partial())
                        .set(REWARD_BATCHES.NAME, entity.name())
                        .set(REWARD_BATCHES.MERCHANT_SEND_DATE, entity.merchantSendDate())
                        .set(REWARD_BATCHES.APPROVAL_DATE, entity.approvalDate())
                        .set(REWARD_BATCHES.DELIVERY_DATE_REQUEST, entity.deliveryDateRequest())
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

    private Mono<RewardBatch> insert(RewardBatchEntity entity) {
        return Mono.from(dslContext.insertInto(REWARD_BATCHES)
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
