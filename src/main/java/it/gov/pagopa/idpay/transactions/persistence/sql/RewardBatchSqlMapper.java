package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Row;
import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDate;
import java.time.LocalDateTime;

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

    RewardBatchEntity entityFromRow(Row row) {
        return new RewardBatchEntity(
                row.get("id", String.class),
                row.get("initiative_id", String.class),
                row.get("merchant_id", String.class),
                row.get("business_name", String.class),
                row.get("month", String.class),
                row.get("pos_type", String.class),
                row.get("status", String.class),
                Boolean.TRUE.equals(row.get("partial", Boolean.class)),
                row.get("name", String.class),
                row.get("start_date", LocalDateTime.class),
                row.get("end_date", LocalDateTime.class),
                row.get("creation_date", LocalDateTime.class),
                row.get("update_date", LocalDateTime.class),
                row.get("merchant_send_date", LocalDateTime.class),
                row.get("approval_date", LocalDateTime.class),
                row.get("delivery_date_request", LocalDateTime.class),
                row.get("refund_outcome_timestamp", LocalDateTime.class),
                row.get("report_path", String.class),
                row.get("filename", String.class),
                row.get("assignee_level", String.class),
                row.get("refund_valuta_date", LocalDate.class),
                row.get("refund_error_message", String.class),
                row.get("delivery_outcome", Json.class),
                false
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
