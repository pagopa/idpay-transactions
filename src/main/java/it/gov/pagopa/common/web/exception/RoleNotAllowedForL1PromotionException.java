package it.gov.pagopa.common.web.exception;

public class RoleNotAllowedForL1PromotionException extends ServiceException {
    public RoleNotAllowedForL1PromotionException(String code, String message) {
        super(code, message);
    }
}
