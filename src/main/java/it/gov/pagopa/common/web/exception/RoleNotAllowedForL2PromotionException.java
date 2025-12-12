package it.gov.pagopa.common.web.exception;

public class RoleNotAllowedForL2PromotionException extends ServiceException {
    public RoleNotAllowedForL2PromotionException(String code, String message) {
        super(code, message);
    }
}
