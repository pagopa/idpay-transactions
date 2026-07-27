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

    RewardBatchRecord toRecord(RewardBatch batch) {
        return new RewardBatchRecord(
                batch.getId(),
                batch.getInitiativeId(),
                batch.getMerchantId(),
                batch.getBusinessName(),
                batch.getMonth(),
                batch.getPosType(),
                batch.getStatus(),
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
                batch.getAssigneeLevel(),
                batch.getRefundValutaDate(),
                batch.getRefundErrorMessage(),
                toJson(batch.getDeliveryOutcome())
        );
    }

    RewardBatch fromRow(Row row) {
        return RewardBatch.builder()
                .id(row.get("id", String.class))
                .initiativeId(row.get("initiative_id", String.class))
                .merchantId(row.get("merchant_id", String.class))
                .businessName(row.get("business_name", String.class))
                .month(row.get("month", String.class))
                .posType(PosType.valueOf(row.get("pos_type", String.class)))
                .status(RewardBatchStatus.valueOf(row.get("status", String.class)))
                .partial(row.get("partial", Boolean.class))
                .name(row.get("name", String.class))
                .startDate(row.get("start_date", LocalDateTime.class))
                .endDate(row.get("end_date", LocalDateTime.class))
                .creationDate(row.get("creation_date", LocalDateTime.class))
                .updateDate(row.get("update_date", LocalDateTime.class))
                .merchantSendDate(row.get("merchant_send_date", LocalDateTime.class))
                .approvalDate(row.get("approval_date", LocalDateTime.class))
                .deliveryDateRequest(row.get("delivery_date_request", LocalDateTime.class))
                .refundOutcomeTimestamp(row.get("refund_outcome_timestamp", LocalDateTime.class))
                .reportPath(row.get("report_path", String.class))
                .filename(row.get("filename", String.class))
                .assigneeLevel(RewardBatchAssignee.valueOf(row.get("assignee_level", String.class)))
                .refundValutaDate(row.get("refund_valuta_date", LocalDate.class))
                .refundErrorMessage(row.get("refund_error_message", String.class))
                .deliveryOutcome(fromJson(row.get("delivery_outcome", Json.class)))
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
