package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import lombok.Data;

@Data
public class PointOfSaleDTO {

  private String id;
  private PointOfSaleTypeEnum type;
}
