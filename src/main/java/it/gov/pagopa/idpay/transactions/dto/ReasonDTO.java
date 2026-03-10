package it.gov.pagopa.idpay.transactions.dto;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReasonDTO {

    @NotNull
    private LocalDateTime date;
    @NotNull
    private String reason;
}
