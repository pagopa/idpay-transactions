package it.gov.pagopa.common.web.exception;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.REWARD_BATCH_INVALID_REQUEST;

public class RewardBatchInvalidRequestException extends ServiceException {

  public RewardBatchInvalidRequestException(String message) {
    this(REWARD_BATCH_INVALID_REQUEST, message, false, null);
  }

  public RewardBatchInvalidRequestException(String code, String message, boolean printStackTrace, Throwable ex) {
    super(code, message, printStackTrace, ex);
  }
}
