package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import org.springframework.stereotype.Component;

@Component
public class ChecksErrorMapper {

    private static final ChecksErrorDTO NOT_CHECKED_ERROR_DTO = new ChecksErrorDTO(false, false, false, false, false, false, false, false);

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
        ce.setGenericError(dto.isGenericError());

        return ce;
    }


    public ChecksErrorDTO toDto(ChecksError checksError) {
        if (checksError == null) {
            return NOT_CHECKED_ERROR_DTO;
        }

        ChecksErrorDTO out = new ChecksErrorDTO();
        out.setCfError(checksError.isCfError());
        out.setProductEligibilityError(checksError.isProductEligibilityError());
        out.setDisposalRaeeError(checksError.isDisposalRaeeError());
        out.setPriceError(checksError.isPriceError());
        out.setBonusError(checksError.isBonusError());
        out.setSellerReferenceError(checksError.isSellerReferenceError());
        out.setAccountingDocumentError(checksError.isAccountingDocumentError());
        out.setGenericError(checksError.isGenericError());

        return out;
    }
}
