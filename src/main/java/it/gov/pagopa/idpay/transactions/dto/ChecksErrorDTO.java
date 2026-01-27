package it.gov.pagopa.idpay.transactions.dto;

import lombok.Data;

@Data
public class ChecksErrorDTO {

    private boolean cfError;
    private boolean productEligibilityError;
    private boolean disposalRaeeError;
    private boolean priceError;
    private boolean bonusError;
    private boolean sellerReferenceError;
    private boolean accountingDocumentError;
}
