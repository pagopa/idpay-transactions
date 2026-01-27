package it.gov.pagopa.idpay.transactions.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChecksError {

    private boolean cfError;
    private boolean productEligibilityError;
    private boolean disposalRaeeError;
    private boolean priceError;
    private boolean bonusError;
    private boolean sellerReferenceError;
    private boolean accountingDocumentError;
}
