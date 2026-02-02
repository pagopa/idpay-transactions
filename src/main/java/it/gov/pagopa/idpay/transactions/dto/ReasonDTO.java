package it.gov.pagopa.idpay.transactions.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReasonDTO {

    private LocalDateTime date;
    private String reason;
}
