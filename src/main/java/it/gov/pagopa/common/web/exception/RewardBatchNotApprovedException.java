package it.gov.pagopa.common.web.exception;

public class RewardBatchNotApprovedException extends ServiceException {
    public RewardBatchNotApprovedException(String code, String message) {
        super(code, message);
    }
}
