package it.gov.pagopa.idpay.transactions.dto;

import io.swagger.v3.oas.annotations.media.Schema;
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

    @Schema(description = "Internal note, visible only to institution operators")
    private String internalNote;

    @Schema(description = "Official note, visible to institution operators and merchants")
    private String officialNote;

    private ChecksErrorDTO checksError;
}
