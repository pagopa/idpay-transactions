package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MerchantDetailDTO {

  private String businessName;
  private String vatNumber;
  private String fiscalCode;
  private String iban;
  private String ibanHolder;
}
