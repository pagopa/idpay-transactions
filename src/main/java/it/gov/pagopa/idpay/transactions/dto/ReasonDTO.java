package it.gov.pagopa.idpay.transactions.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReasonDTO {

    private OffsetDateTime date;
    private String reason;
}
