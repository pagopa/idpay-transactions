package it.gov.pagopa.common.web.exception;

public class RoleNotAllowedException extends ServiceException {
    public RoleNotAllowedException(String code, String message) {
        super(code, message);
    }
}
