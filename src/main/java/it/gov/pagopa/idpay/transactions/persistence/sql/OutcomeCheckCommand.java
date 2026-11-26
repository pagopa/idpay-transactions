package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import java.time.LocalDate;

public record OutcomeCheckCommand(
        String invitaliaStatus,
        Long amountPaidCents,
        LocalDate refundValutaDate,
        String refundErrorMessage,
        RewardBatchStatus targetStatusIfAny
) {
}