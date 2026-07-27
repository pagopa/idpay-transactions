package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Table("reward_batches")
public record RewardBatchEntity(
        @Id String id,
        @Column("initiative_id") String initiativeId,
        @Column("merchant_id") String merchantId,
        @Column("business_name") String businessName,
        String month,
        @Column("pos_type") String posType,
        String status,
        boolean partial,
        String name,
        @Column("start_date") LocalDateTime startDate,
        @Column("end_date") LocalDateTime endDate,
        @Column("creation_date") LocalDateTime creationDate,
        @Column("update_date") LocalDateTime updateDate,
        @Column("merchant_send_date") LocalDateTime merchantSendDate,
        @Column("approval_date") LocalDateTime approvalDate,
        @Column("delivery_date_request") LocalDateTime deliveryDateRequest,
        @Column("refund_outcome_timestamp") LocalDateTime refundOutcomeTimestamp,
        @Column("report_path") String reportPath,
        String filename,
        @Column("assignee_level") String assigneeLevel,
        @Column("refund_valuta_date") LocalDate refundValutaDate,
        @Column("refund_error_message") String refundErrorMessage,
        @Column("delivery_outcome") Json deliveryOutcome,
        @Transient boolean newEntity
) implements Persistable<String> {

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return newEntity;
    }
}
