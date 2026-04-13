package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class InitiativeDetailResponseDTO {
    String initiativeId;
    InitiativeGeneralResponseDTO general;
}
