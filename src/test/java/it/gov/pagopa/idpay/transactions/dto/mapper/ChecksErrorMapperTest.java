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
        dto.setPrice(false);
        dto.setBonus(false);
        dto.setSellerReference(false);
        dto.setAccountingDocument(false);

        ChecksError result = mapper.toModel(dto);

        assertFalse(result.isCfError());
        assertFalse(result.isProductEligibilityError());
        assertFalse(result.isDisposalRaeeError());
        assertFalse(result.isPrice());
        assertFalse(result.isBonus());
        assertFalse(result.isSellerReference());
        assertFalse(result.isAccountingDocument());
    }

    @Test
    void toModel_someFieldsTrue() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        dto.setProductEligibilityError(false);
        dto.setDisposalRaeeError(true);
        dto.setPrice(false);
        dto.setBonus(true);
        dto.setSellerReference(false);
        dto.setAccountingDocument(true);

        ChecksError result = mapper.toModel(dto);

        assertTrue(result.isCfError());
        assertFalse(result.isProductEligibilityError());
        assertTrue(result.isDisposalRaeeError());
        assertFalse(result.isPrice());
        assertTrue(result.isBonus());
        assertFalse(result.isSellerReference());
        assertTrue(result.isAccountingDocument());
    }

    @Test
    void toModel_allFieldsTrue() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        dto.setProductEligibilityError(true);
        dto.setDisposalRaeeError(true);
        dto.setPrice(true);
        dto.setBonus(true);
        dto.setSellerReference(true);
        dto.setAccountingDocument(true);

        ChecksError result = mapper.toModel(dto);

        assertTrue(result.isCfError());
        assertTrue(result.isProductEligibilityError());
        assertTrue(result.isDisposalRaeeError());
        assertTrue(result.isPrice());
        assertTrue(result.isBonus());
        assertTrue(result.isSellerReference());
        assertTrue(result.isAccountingDocument());
    }
}
