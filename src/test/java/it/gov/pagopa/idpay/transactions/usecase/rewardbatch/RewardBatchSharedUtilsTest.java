package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.InvalidChecksErrorException;
import it.gov.pagopa.idpay.transactions.dto.ChecksErrorDTO;
import org.junit.jupiter.api.Test;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionMessage.ERROR_MESSAGE_INVALID_CHECKS_ERROR;
import static org.junit.jupiter.api.Assertions.*;

class RewardBatchSharedUtilsTest {

    @Test
    void isOperator_checks() {
        assertFalse(RewardBatchSharedUtils.isOperator(null));
        assertFalse(RewardBatchSharedUtils.isOperator("guest"));
        assertTrue(RewardBatchSharedUtils.isOperator("operator1"));
        assertTrue(RewardBatchSharedUtils.isOperator("OPERATOR2"));
    }

    @Test
    void validChecksError_null_ok() {
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(null));
    }

    @Test
    void validChecksError_allFalse_throws() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(false);
        dto.setProductEligibilityError(false);
        dto.setDisposalRaeeError(false);
        dto.setPriceError(false);
        dto.setBonusError(false);
        dto.setSellerReferenceError(false);
        dto.setAccountingDocumentError(false);

        InvalidChecksErrorException ex = assertThrows(InvalidChecksErrorException.class, () -> RewardBatchSharedUtils.validChecksError(dto));
        assertEquals(ERROR_MESSAGE_INVALID_CHECKS_ERROR, ex.getMessage());
    }

    @Test
    void validChecksError_anyTrue_ok() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setCfError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void validChecksError_productEligibilityError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setProductEligibilityError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void validChecksError_disposalRaeeError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setDisposalRaeeError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void validChecksError_priceError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setPriceError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void validChecksError_bonusError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setBonusError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void validChecksError_sellerReferenceError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setSellerReferenceError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void validChecksError_accountingDocumentError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setAccountingDocumentError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void validChecksError_genericError_true_doesNotThrow() {
        ChecksErrorDTO dto = new ChecksErrorDTO();
        dto.setGenericError(true);
        assertDoesNotThrow(() -> RewardBatchSharedUtils.validChecksError(dto));
    }

    @Test
    void addOneMonth_and_italian() {
        assertEquals("2026-01", RewardBatchSharedUtils.addOneMonth("2025-12"));
        assertEquals("gennaio 2026", RewardBatchSharedUtils.addOneMonthToItalian("dicembre 2025"));
    }
}

