package it.gov.pagopa.common.web.exception;

public class RewardBatchMissingFilenameException extends ServiceException {
    public RewardBatchMissingFilenameException(String code, String message) {
        super(code, message);
    }
}
