package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChecksErrorMapperTest {

    private ChecksErrorMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ChecksErrorMapper();
    }

    @Test
    void toModel_nullInput_returnsNull() {
        ChecksError result = mapper.toModel(null);
        assertNull(result);
    }

    @Test
    void toModel_allFieldsFalse() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(false);
        dto.setProductEligibilityError(false);
        dto.setDisposalRaeeError(false);
        dto.setPriceError(false);
        dto.setBonusError(false);
        dto.setSellerReferenceError(false);
        dto.setAccountingDocumentError(false);

        ChecksError result = mapper.toModel(dto);

        assertFalse(result.isCfError());
        assertFalse(result.isProductEligibilityError());
        assertFalse(result.isDisposalRaeeError());
        assertFalse(result.isPriceError());
        assertFalse(result.isBonusError());
        assertFalse(result.isSellerReferenceError());
        assertFalse(result.isAccountingDocumentError());
    }

    @Test
    void toModel_someFieldsTrue() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        dto.setProductEligibilityError(false);
        dto.setDisposalRaeeError(true);
        dto.setPriceError(false);
        dto.setBonusError(true);
        dto.setSellerReferenceError(false);
        dto.setAccountingDocumentError(true);

        ChecksError result = mapper.toModel(dto);

        assertTrue(result.isCfError());
        assertFalse(result.isProductEligibilityError());
        assertTrue(result.isDisposalRaeeError());
        assertFalse(result.isPriceError());
        assertTrue(result.isBonusError());
        assertFalse(result.isSellerReferenceError());
        assertTrue(result.isAccountingDocumentError());
    }

    @Test
    void toModel_allFieldsTrue() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        dto.setProductEligibilityError(true);
        dto.setDisposalRaeeError(true);
        dto.setPriceError(true);
        dto.setBonusError(true);
        dto.setSellerReferenceError(true);
        dto.setAccountingDocumentError(true);

        ChecksError result = mapper.toModel(dto);

        assertTrue(result.isCfError());
        assertTrue(result.isProductEligibilityError());
        assertTrue(result.isDisposalRaeeError());
        assertTrue(result.isPriceError());
        assertTrue(result.isBonusError());
        assertTrue(result.isSellerReferenceError());
        assertTrue(result.isAccountingDocumentError());
    }
}
