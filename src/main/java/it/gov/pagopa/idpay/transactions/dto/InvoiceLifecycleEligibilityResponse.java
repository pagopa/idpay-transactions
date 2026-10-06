package it.gov.pagopa.idpay.transactions.dto;

import it.gov.pagopa.idpay.transactions.enums.InvoiceLifecycleEligibilityDecision;

public record InvoiceLifecycleEligibilityResponse(
        InvoiceLifecycleEligibilityDecision decision
) {
}
