package it.gov.pagopa.common.web.exception;

public class InvalidBatchStateForPromotionException extends ServiceException {
    public InvalidBatchStateForPromotionException(String code, String message) {
        super(code, message);
    }
}
