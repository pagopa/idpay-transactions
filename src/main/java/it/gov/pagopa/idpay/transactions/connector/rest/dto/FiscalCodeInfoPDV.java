package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FiscalCodeInfoPDV {

  private String token;

}
