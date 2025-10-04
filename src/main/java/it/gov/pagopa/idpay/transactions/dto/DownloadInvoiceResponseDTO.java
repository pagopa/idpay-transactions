package it.gov.pagopa.idpay.transactions.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DownloadInvoiceResponseDTO {
  private String invoiceUrl;
}
