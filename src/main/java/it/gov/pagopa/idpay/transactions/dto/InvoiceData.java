package it.gov.pagopa.idpay.transactions.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import lombok.experimental.SuperBuilder;


@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
@FieldNameConstants()
public class InvoiceData {

    private String filename;
    private String docNumber;

}
