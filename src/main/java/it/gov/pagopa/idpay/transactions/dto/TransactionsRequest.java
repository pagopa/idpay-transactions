package it.gov.pagopa.idpay.transactions.dto;

import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransactionsRequest {

    @NotEmpty
    private List<String> transactionIds;

    private List<ReasonDTO> reasons;

    private ChecksErrorDTO checksError;
}
