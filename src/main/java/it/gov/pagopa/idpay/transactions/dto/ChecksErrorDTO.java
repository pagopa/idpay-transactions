package it.gov.pagopa.idpay.transactions.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChecksErrorDTO {

    private boolean cfError;
    private boolean productEligibilityError;
    private boolean disposalRaeeError;
    private boolean priceError;
    private boolean bonusError;
    private boolean sellerReferenceError;
    private boolean accountingDocumentError;
    private boolean genericError;
}
