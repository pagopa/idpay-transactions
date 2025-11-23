package it.gov.pagopa.common.web.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class RewardBatchException extends RuntimeException{
    private final HttpStatus httpStatus;
    private final boolean printStackTrace;

    public RewardBatchException(HttpStatus httpStatus, String message){
        this(httpStatus, message, null);
    }

    public RewardBatchException(HttpStatus httpStatus, String message, Throwable ex){
        this(httpStatus, message, false, ex);
    }

    public RewardBatchException(HttpStatus httpStatus, String message, boolean printStackTrace, Throwable ex){
        super(message, ex);
        this.httpStatus=httpStatus;
        this.printStackTrace=printStackTrace;
    }
}
