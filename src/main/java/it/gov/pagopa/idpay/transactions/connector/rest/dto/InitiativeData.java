package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import java.time.LocalDate;

public record InitiativeData(
        String initiativeId,
        LocalDate initiativeStartDate,
        LocalDate initiativeEndDate
) {
}