package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GetTransactionsProjectionRequestDTO {
    private Set<String> transactionIds;
}

