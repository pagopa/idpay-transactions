package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;

@RequiredArgsConstructor
public class SqlRewardBatchAdapter {

    private static final String BATCH_COLUMNS = """
            id, initiative_id, merchant_id, business_name, month, pos_type, status, partial, name,
            start_date, end_date, creation_date, update_date, merchant_send_date, approval_date,
            delivery_date_request, refund_outcome_timestamp, report_path, filename, assignee_level,
            refund_valuta_date, refund_error_message, delivery_outcome
            """;

    private final DatabaseClient databaseClient;
    private final TransactionalOperator transactionalOperator;
    private final RewardBatchSqlMapper mapper;

    public Mono<RewardBatch> findById(String id) {
        return databaseClient.sql("SELECT " + BATCH_COLUMNS + " FROM reward_batches WHERE id = :id")
                .bind("id", id)
                .map((row, metadata) -> mapper.fromRow(row))
                .one();
    }

    public Mono<RewardBatch> findByIdAndInitiativeId(String id, String initiativeId) {
        return databaseClient.sql("""
                        SELECT %s FROM reward_batches
                        WHERE id = :id AND initiative_id = :initiativeId
                        """.formatted(BATCH_COLUMNS))
                .bind("id", id)
                .bind("initiativeId", initiativeId)
                .map((row, metadata) -> mapper.fromRow(row))
                .one();
    }

    public Mono<RewardBatch> findByMerchantInitiativeAndId(
            String merchantId,
            String initiativeId,
            String id
    ) {
        return databaseClient.sql("""
                        SELECT %s FROM reward_batches
                        WHERE id = :id AND initiative_id = :initiativeId AND merchant_id = :merchantId
                        """.formatted(BATCH_COLUMNS))
                .bind("id", id)
                .bind("initiativeId", initiativeId)
                .bind("merchantId", merchantId)
                .map((row, metadata) -> mapper.fromRow(row))
                .one();
    }

    public Mono<RewardBatch> findByGrouping(
            String initiativeId,
            String merchantId,
            PosType posType,
            String month
    ) {
        return databaseClient.sql("""
                        SELECT %s FROM reward_batches
                        WHERE initiative_id = :initiativeId
                          AND merchant_id = :merchantId
                          AND pos_type = :posType
                          AND month = :month
                        """.formatted(BATCH_COLUMNS))
                .bind("initiativeId", initiativeId)
                .bind("merchantId", merchantId)
                .bind("posType", posType.name())
                .bind("month", month)
                .map((row, metadata) -> mapper.fromRow(row))
                .one();
    }

    public Mono<RewardBatch> createOrRead(RewardBatch batch) {
        RewardBatchRecord record = mapper.toRecord(batch);
        return transactionalOperator.transactional(insert(record)
                .switchIfEmpty(findByGrouping(
                        record.initiativeId(),
                        record.merchantId(),
                        record.posType(),
                        record.month()
                )));
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
                        RETURNING %s
                        """.formatted(BATCH_COLUMNS))
                .bind("status", status.name())
                .bind("id", id)
                .bind("initiativeId", initiativeId)
                .map((row, metadata) -> mapper.fromRow(row))
                .one());
    }

    public Mono<RewardBatch> updateMetadata(RewardBatch batch) {
        RewardBatchRecord record = mapper.toRecord(batch);
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
                        RETURNING %s
                        """.formatted(BATCH_COLUMNS)), record)
                .map((row, metadata) -> mapper.fromRow(row))
                .one());
    }

    private Mono<RewardBatch> insert(RewardBatchRecord record) {
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
                        RETURNING %s
                        """.formatted(BATCH_COLUMNS)), record)
                .map((row, metadata) -> mapper.fromRow(row))
                .one();
    }

    private static DatabaseClient.GenericExecuteSpec bindInsert(
            DatabaseClient.GenericExecuteSpec spec,
            RewardBatchRecord record
    ) {
        return bindMetadata(spec, record)
                .bind("month", record.month())
                .bind("posType", record.posType().name())
                .bind("status", record.status().name())
                .bind("id", record.id())
                .bind("initiativeId", record.initiativeId())
                .bind("merchantId", record.merchantId());
    }

    private static DatabaseClient.GenericExecuteSpec bindMetadata(
            DatabaseClient.GenericExecuteSpec spec,
            RewardBatchRecord record
    ) {
        spec = bind(spec, "businessName", record.businessName(), String.class);
        spec = bind(spec, "partial", record.partial(), Boolean.class);
        spec = bind(spec, "name", record.name(), String.class);
        spec = bind(spec, "merchantSendDate", record.merchantSendDate(), LocalDateTime.class);
        spec = bind(spec, "approvalDate", record.approvalDate(), LocalDateTime.class);
        spec = bind(spec, "deliveryDateRequest", record.deliveryDateRequest(), LocalDateTime.class);
        spec = bind(spec, "refundOutcomeTimestamp", record.refundOutcomeTimestamp(), LocalDateTime.class);
        spec = bind(spec, "reportPath", record.reportPath(), String.class);
        spec = bind(spec, "filename", record.filename(), String.class);
        spec = bind(spec, "refundValutaDate", record.refundValutaDate(), LocalDate.class);
        spec = bind(spec, "refundErrorMessage", record.refundErrorMessage(), String.class);
        spec = bind(spec, "deliveryOutcome", record.deliveryOutcome(), io.r2dbc.postgresql.codec.Json.class);
        spec = bind(spec, "assigneeLevel", record.assigneeLevel().name(), String.class);
        spec = bind(spec, "startDate", record.startDate(), LocalDateTime.class);
        spec = bind(spec, "endDate", record.endDate(), LocalDateTime.class);
        spec = bind(spec, "creationDate", record.creationDate(), LocalDateTime.class);
        spec = bind(spec, "updateDate", record.updateDate(), LocalDateTime.class);
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
