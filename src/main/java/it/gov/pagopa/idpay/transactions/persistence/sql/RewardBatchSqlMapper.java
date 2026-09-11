package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.postgresql.codec.Json;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import lombok.RequiredArgsConstructor;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardBatchesRecord;
import org.springframework.stereotype.Component;
import org.jooq.Record;
import org.jooq.JSONB;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class RewardBatchSqlMapper {

    private final ObjectMapper objectMapper;

    RewardBatchEntity toEntity(RewardBatch batch) {
        return new RewardBatchEntity(
                batch.getId(),
                batch.getInitiativeId(),
                batch.getMerchantId(),
                batch.getBusinessName(),
                batch.getMonth(),
                batch.getPosType().name(),
                batch.getStatus().name(),
                Boolean.TRUE.equals(batch.getPartial()),
                batch.getName(),
                batch.getStartDate(),
                batch.getEndDate(),
                batch.getCreationDate(),
                batch.getUpdateDate(),
                batch.getMerchantSendDate(),
                batch.getApprovalDate(),
                batch.getDeliveryDateRequest(),
                batch.getDeliveryAmountCents(),
                null,
                null,
                batch.getRefundOutcomeTimestamp(),
                batch.getReportPath(),
                batch.getFilename(),
                batch.getAssigneeLevel().name(),
                batch.getRefundValutaDate(),
                batch.getRefundErrorMessage(),
                toJson(batch.getDeliveryOutcome())
        );
    }

    RewardBatch fromEntity(RewardBatchEntity entity) {
        return RewardBatch.builder()
                .id(entity.id())
                .initiativeId(entity.initiativeId())
                .merchantId(entity.merchantId())
                .businessName(entity.businessName())
                .month(entity.month())
                .posType(PosType.valueOf(entity.posType()))
                .status(RewardBatchStatus.valueOf(entity.status()))
                .partial(entity.partial())
                .name(entity.name())
                .startDate(entity.startDate())
                .endDate(entity.endDate())
                .creationDate(entity.creationDate())
                .updateDate(entity.updateDate())
                .merchantSendDate(entity.merchantSendDate())
                .approvalDate(entity.approvalDate())
                .deliveryDateRequest(entity.deliveryDateRequest())
                .deliveryAmountCents(entity.deliveryAmountCents())
                .refundOutcomeTimestamp(entity.refundOutcomeTimestamp())
                .reportPath(entity.reportPath())
                .filename(entity.filename())
                .assigneeLevel(RewardBatchAssignee.valueOf(entity.assigneeLevel()))
                .refundValutaDate(entity.refundValutaDate())
                .refundErrorMessage(entity.refundErrorMessage())
                .deliveryOutcome(fromJson(entity.deliveryOutcome()))
                .build();
    }

    RewardBatch fromRecord(RewardBatchesRecord batchRecord) {
        return fromEntity(new RewardBatchEntity(
                batchRecord.getId(),
                batchRecord.getInitiativeId(),
                batchRecord.getMerchantId(),
                batchRecord.getBusinessName(),
                batchRecord.getMonth(),
                batchRecord.getPosType(),
                batchRecord.getStatus(),
                batchRecord.getPartial(),
                batchRecord.getName(),
                batchRecord.getStartDate(),
                batchRecord.getEndDate(),
                batchRecord.getCreationDate(),
                batchRecord.getUpdateDate(),
                batchRecord.getMerchantSendDate(),
                batchRecord.getApprovalDate(),
                batchRecord.getDeliveryDateRequest(),
                batchRecord.getDeliveryAmountCents(),
                batchRecord.getInitialAmountCentsAtSend(),
                batchRecord.getSuspendedAmountCentsAtApproving(),
                batchRecord.getRefundOutcomeTimestamp(),
                batchRecord.getReportPath(),
                batchRecord.getFilename(),
                batchRecord.getAssigneeLevel(),
                batchRecord.getRefundValutaDate(),
                batchRecord.getRefundErrorMessage(),
                batchRecord.getDeliveryOutcome() == null ? null : Json.of(batchRecord.getDeliveryOutcome().data())
        ));
    }

    RewardBatch fromAggregateRecord(
            Record result,
            BatchAggregateProjection aggregateProjection
    ) {
        RewardBatchesRecord batchRecord = result.into(RewardBatchesRecord.class);
        RewardBatch batch = fromRecord(batchRecord);
        validateRequiredSnapshots(batchRecord);
        batch.setNumberOfTransactions(result.get(aggregateProjection.numberOfTransactions()));
        batch.setInitialAmountCents(result.get(aggregateProjection.initialAmountCents()));
        batch.setNumberOfTransactionsElaborated(result.get(aggregateProjection.numberOfTransactionsElaborated()));
        batch.setNumberOfTransactionsSuspended(result.get(aggregateProjection.numberOfTransactionsSuspended()));
        batch.setNumberOfTransactionsRejected(result.get(aggregateProjection.numberOfTransactionsRejected()));
        batch.setSuspendedAmountCents(result.get(aggregateProjection.suspendedAmountCents()));
        batch.setApprovedAmountCents(result.get(aggregateProjection.approvedAmountCents()));
        batch.setCurrentAmountCents(result.get(aggregateProjection.currentAmountCents()));
        batch.setExcludedAmountCents(result.get(aggregateProjection.excludedAmountCents()));
        return batch;
    }

    record BatchAggregateProjection(
            org.jooq.Field<Long> numberOfTransactions,
            org.jooq.Field<Long> initialAmountCents,
            org.jooq.Field<Long> numberOfTransactionsElaborated,
            org.jooq.Field<Long> numberOfTransactionsSuspended,
            org.jooq.Field<Long> numberOfTransactionsRejected,
            org.jooq.Field<Long> suspendedAmountCents,
            org.jooq.Field<Long> approvedAmountCents,
            org.jooq.Field<Long> currentAmountCents,
            org.jooq.Field<Long> excludedAmountCents
    ) {
    }

    private static void validateRequiredSnapshots(RewardBatchesRecord batchRecord) {
        RewardBatchStatus status = RewardBatchStatus.valueOf(batchRecord.getStatus());
        if (status != RewardBatchStatus.CREATED && batchRecord.getInitialAmountCentsAtSend() == null) {
            throw missingSnapshot(batchRecord, "initial_amount_cents_at_send");
        }
        if (requiresSuspendedSnapshot(status) && batchRecord.getSuspendedAmountCentsAtApproving() == null) {
            throw missingSnapshot(batchRecord, "suspended_amount_cents_at_approving");
        }
    }

    private static boolean requiresSuspendedSnapshot(RewardBatchStatus status) {
        return switch (status) {
            case APPROVING, APPROVED, PENDING_REFUND, NOT_REFUNDED, REFUNDED -> true;
            default -> false;
        };
    }

    private static IllegalStateException missingSnapshot(
            RewardBatchesRecord batchRecord,
            String snapshotColumn
    ) {
        return new IllegalStateException(
                "Data integrity error: reward batch %s is missing required snapshot %s"
                        .formatted(batchRecord.getId(), snapshotColumn)
        );
    }

    private Json toJson(DeliveryOutcomeDTO deliveryOutcome) {
        if (deliveryOutcome == null) {
            return null;
        }
        try {
            return Json.of(objectMapper.writeValueAsString(deliveryOutcome));
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Unable to serialize delivery outcome", exception);
        }
    }

    JSONB toJooqJsonb(DeliveryOutcomeDTO deliveryOutcome) {
        Json json = toJson(deliveryOutcome);
        return json == null ? null : JSONB.jsonb(json.asString());
    }

    private DeliveryOutcomeDTO fromJson(Json deliveryOutcome) {
        if (deliveryOutcome == null) {
            return null;
        }
        try {
            return objectMapper.readValue(deliveryOutcome.asString(), DeliveryOutcomeDTO.class);
        } catch (JacksonException exception) {
            throw new IllegalStateException("Unable to deserialize delivery outcome", exception);
        }
    }
}
