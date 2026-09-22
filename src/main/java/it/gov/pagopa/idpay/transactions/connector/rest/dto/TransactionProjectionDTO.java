package it.gov.pagopa.idpay.transactions.connector.rest.dto;

import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TransactionProjectionDTO {
    private String transactionId;
    private String status;
    private InvoiceData invoiceData;
}

