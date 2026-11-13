package it.gov.pagopa.common.web.exception;

public class BatchControlThresholdNotMetException extends ServiceException {
    public BatchControlThresholdNotMetException(String code, String message) {
        super(code, message);
    }
}
