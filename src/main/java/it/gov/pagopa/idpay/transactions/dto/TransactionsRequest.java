package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

    /**
     * Legacy alias for the internal note. Kept to avoid breaking existing clients while
     * they migrate to {@code internalNote}.
     */
    @Deprecated
    @Schema(deprecated = true, description = "Legacy alias for internalNote")
    private String reason;

    private ChecksErrorDTO checksError;

    @JsonIgnore
    public String getResolvedInternalNote() {
        return internalNote != null ? internalNote : reason;
    }
}
