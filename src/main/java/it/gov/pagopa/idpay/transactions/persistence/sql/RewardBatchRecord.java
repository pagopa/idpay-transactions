package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.postgresql.codec.Json;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;

import java.time.LocalDate;
import java.time.LocalDateTime;

record RewardBatchRecord(
        String id,
        String initiativeId,
        String merchantId,
        String businessName,
        String month,
        PosType posType,
        RewardBatchStatus status,
        boolean partial,
        String name,
        LocalDateTime startDate,
        LocalDateTime endDate,
        LocalDateTime creationDate,
        LocalDateTime updateDate,
        LocalDateTime merchantSendDate,
        LocalDateTime approvalDate,
        LocalDateTime deliveryDateRequest,
        LocalDateTime refundOutcomeTimestamp,
        String reportPath,
        String filename,
        RewardBatchAssignee assigneeLevel,
        LocalDate refundValutaDate,
        String refundErrorMessage,
        Json deliveryOutcome
) {
}
