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
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class RewardBatchSqlMapper {

    private final ObjectMapper objectMapper;

    RewardBatchEntity toEntity(RewardBatch batch, boolean newEntity) {
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
                batch.getRefundOutcomeTimestamp(),
                batch.getReportPath(),
                batch.getFilename(),
                batch.getAssigneeLevel().name(),
                batch.getRefundValutaDate(),
                batch.getRefundErrorMessage(),
                toJson(batch.getDeliveryOutcome()),
                newEntity
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
                .refundOutcomeTimestamp(entity.refundOutcomeTimestamp())
                .reportPath(entity.reportPath())
                .filename(entity.filename())
                .assigneeLevel(RewardBatchAssignee.valueOf(entity.assigneeLevel()))
                .refundValutaDate(entity.refundValutaDate())
                .refundErrorMessage(entity.refundErrorMessage())
                .deliveryOutcome(fromJson(entity.deliveryOutcome()))
                .build();
    }

    RewardBatch fromRecord(RewardBatchesRecord record) {
        return fromEntity(new RewardBatchEntity(
                record.getId(),
                record.getInitiativeId(),
                record.getMerchantId(),
                record.getBusinessName(),
                record.getMonth(),
                record.getPosType(),
                record.getStatus(),
                Boolean.TRUE.equals(record.getPartial()),
                record.getName(),
                record.getStartDate(),
                record.getEndDate(),
                record.getCreationDate(),
                record.getUpdateDate(),
                record.getMerchantSendDate(),
                record.getApprovalDate(),
                record.getDeliveryDateRequest(),
                record.getRefundOutcomeTimestamp(),
                record.getReportPath(),
                record.getFilename(),
                record.getAssigneeLevel(),
                record.getRefundValutaDate(),
                record.getRefundErrorMessage(),
                record.getDeliveryOutcome() == null ? null : Json.of(record.getDeliveryOutcome().data()),
                false
        ));
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
