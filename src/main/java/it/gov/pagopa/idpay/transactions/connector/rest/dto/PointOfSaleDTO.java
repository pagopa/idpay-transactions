package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PointOfSaleDTO {

  private PointOfSaleTypeEnum type;
  private String franchiseName;
  private String businessName;

}
