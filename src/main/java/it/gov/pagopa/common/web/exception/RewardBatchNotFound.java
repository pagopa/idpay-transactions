package it.gov.pagopa.common.web.exception;

public class RewardBatchNotFound extends ServiceException {

  public RewardBatchNotFound(String code, String message) {
    this(code, message, false, null);
  }

  public RewardBatchNotFound(String code, String message, boolean printStackTrace, Throwable ex) {
    super(code, message, printStackTrace, ex);
  }
}
