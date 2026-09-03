package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateTransactionsStatusRequestDTO {
    private Set<String> transactionIds;
    private SyncTrxStatus status;
}

