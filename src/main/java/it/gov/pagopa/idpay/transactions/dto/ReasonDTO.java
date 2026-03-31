package it.gov.pagopa.idpay.transactions.dto;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReasonDTO {

    @NotNull
    private Instant date;
    @NotNull
    private String reason;
}
