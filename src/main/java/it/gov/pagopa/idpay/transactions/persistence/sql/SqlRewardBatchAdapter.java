package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SqlRewardBatchAdapter {

    private final DatabaseClient databaseClient;
    private final TransactionalOperator transactionalOperator;
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
        return transactionalOperator.transactional(databaseClient.sql("""
                        UPDATE reward_batches
                        SET status = :status, update_date = CURRENT_TIMESTAMP
                        WHERE id = :id AND initiative_id = :initiativeId
                        RETURNING *
                        """)
                .bind("status", status.name())
                .bind("id", id)
                .bind("initiativeId", initiativeId)
                .map((row, metadata) -> mapper.fromEntity(mapper.entityFromRow(row)))
                .one());
    }

    public Mono<RewardBatch> updateMetadata(RewardBatch batch) {
        RewardBatchEntity entity = mapper.toEntity(batch, false);
        return transactionalOperator.transactional(bindMetadata(databaseClient.sql("""
                        UPDATE reward_batches
                        SET business_name = :businessName,
                            partial = :partial,
                            name = :name,
                            merchant_send_date = :merchantSendDate,
                            approval_date = :approvalDate,
                            delivery_date_request = :deliveryDateRequest,
                            refund_outcome_timestamp = :refundOutcomeTimestamp,
                            report_path = :reportPath,
                            filename = :filename,
                            assignee_level = :assigneeLevel,
                            refund_valuta_date = :refundValutaDate,
                            refund_error_message = :refundErrorMessage,
                            delivery_outcome = :deliveryOutcome,
                            update_date = CURRENT_TIMESTAMP
                        WHERE id = :id AND initiative_id = :initiativeId
                        RETURNING *
                        """), entity)
                .map((row, metadata) -> mapper.fromEntity(mapper.entityFromRow(row)))
                .one());
    }

    private Mono<RewardBatch> insert(RewardBatchEntity entity) {
        return bindInsert(databaseClient.sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, business_name, month, pos_type, status, partial, name,
                            start_date, end_date, creation_date, update_date, merchant_send_date, approval_date,
                            delivery_date_request, refund_outcome_timestamp, report_path, filename, assignee_level,
                            refund_valuta_date, refund_error_message, delivery_outcome
                        ) VALUES (
                            :id, :initiativeId, :merchantId, :businessName, :month, :posType, :status, :partial, :name,
                            :startDate, :endDate, :creationDate, :updateDate, :merchantSendDate, :approvalDate,
                            :deliveryDateRequest, :refundOutcomeTimestamp, :reportPath, :filename, :assigneeLevel,
                            :refundValutaDate, :refundErrorMessage, :deliveryOutcome
                        )
                        ON CONFLICT (initiative_id, merchant_id, pos_type, month) DO NOTHING
                        RETURNING *
                        """), entity)
                .map((row, metadata) -> mapper.fromEntity(mapper.entityFromRow(row)))
                .one();
    }

    private static DatabaseClient.GenericExecuteSpec bindInsert(
            DatabaseClient.GenericExecuteSpec spec,
            RewardBatchEntity entity
    ) {
        return bindMetadata(spec, entity)
                .bind("month", entity.month())
                .bind("posType", entity.posType())
                .bind("status", entity.status())
                .bind("id", entity.id())
                .bind("initiativeId", entity.initiativeId())
                .bind("merchantId", entity.merchantId());
    }

    private static DatabaseClient.GenericExecuteSpec bindMetadata(
            DatabaseClient.GenericExecuteSpec spec,
            RewardBatchEntity entity
    ) {
        spec = bind(spec, "businessName", entity.businessName(), String.class);
        spec = bind(spec, "partial", entity.partial(), Boolean.class);
        spec = bind(spec, "name", entity.name(), String.class);
        spec = bind(spec, "merchantSendDate", entity.merchantSendDate(), java.time.LocalDateTime.class);
        spec = bind(spec, "approvalDate", entity.approvalDate(), java.time.LocalDateTime.class);
        spec = bind(spec, "deliveryDateRequest", entity.deliveryDateRequest(), java.time.LocalDateTime.class);
        spec = bind(spec, "refundOutcomeTimestamp", entity.refundOutcomeTimestamp(), java.time.LocalDateTime.class);
        spec = bind(spec, "reportPath", entity.reportPath(), String.class);
        spec = bind(spec, "filename", entity.filename(), String.class);
        spec = bind(spec, "refundValutaDate", entity.refundValutaDate(), java.time.LocalDate.class);
        spec = bind(spec, "refundErrorMessage", entity.refundErrorMessage(), String.class);
        spec = bind(spec, "deliveryOutcome", entity.deliveryOutcome(), io.r2dbc.postgresql.codec.Json.class);
        spec = bind(spec, "assigneeLevel", entity.assigneeLevel(), String.class);
        spec = bind(spec, "startDate", entity.startDate(), java.time.LocalDateTime.class);
        spec = bind(spec, "endDate", entity.endDate(), java.time.LocalDateTime.class);
        spec = bind(spec, "creationDate", entity.creationDate(), java.time.LocalDateTime.class);
        spec = bind(spec, "updateDate", entity.updateDate(), java.time.LocalDateTime.class);
        return spec;
    }

    private static DatabaseClient.GenericExecuteSpec bind(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            Object value,
            Class<?> type
    ) {
        return value == null ? spec.bindNull(name, type) : spec.bind(name, value);
    }
}
