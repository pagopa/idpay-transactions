package it.gov.pagopa.common.web.exception;

import static it.gov.pagopa.idpay.transactions.utils.ExceptionConstants.ExceptionCode.INVALID_CHECKS_ERROR;

public class InvalidChecksErrorException extends ServiceException {

    public InvalidChecksErrorException(String message) {
        this(INVALID_CHECKS_ERROR, message, false, null);
    }

    public InvalidChecksErrorException(
            String code,
            String message,
            boolean printStackTrace,
            Throwable ex
    ) {
        super(code, message, printStackTrace, ex);
    }
}
