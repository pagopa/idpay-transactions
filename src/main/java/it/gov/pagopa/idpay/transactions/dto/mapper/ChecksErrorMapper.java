package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import org.springframework.stereotype.Component;

@Component
public class ChecksErrorMapper {

    public ChecksError toModel(ChecksErrorDTO dto) {
        if (dto == null) {
            return null;
        }

        ChecksError ce = new ChecksError();
        ce.setCfError(dto.isCfError());
        ce.setProductEligibilityError(dto.isProductEligibilityError());
        ce.setDisposalRaeeError(dto.isDisposalRaeeError());
        ce.setPriceError(dto.isPriceError());
        ce.setBonusError(dto.isBonusError());
        ce.setSellerReferenceError(dto.isSellerReferenceError());
        ce.setAccountingDocumentError(dto.isAccountingDocumentError());

        return ce;
    }
}
