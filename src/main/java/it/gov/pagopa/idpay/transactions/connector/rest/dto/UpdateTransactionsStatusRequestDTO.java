package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import lombok.Builder;

import java.util.Set;

@Builder
public record UpdateTransactionsStatusRequestDTO(
        Set<String> transactionIds,
        SyncTrxStatus status
) {
}