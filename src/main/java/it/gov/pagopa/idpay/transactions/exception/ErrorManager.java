package it.gov.pagopa.idpay.transactions.exception;

import it.gov.pagopa.idpay.transactions.dto.ErrorDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;


@RestControllerAdvice
@Slf4j
public class ErrorManager {
    private static final ErrorDTO defaultErrorDTO;
    static {
        defaultErrorDTO =new ErrorDTO(Severity.ERROR, "Error", "Something gone wrong");
    }
    @ExceptionHandler(RuntimeException.class)
    protected ResponseEntity<?> handleException(RuntimeException error, ServerWebExchange exchange) {
        if(!(error instanceof ClientException) || ((ClientException)error).isPrintStackTrace()){
            log.error("Something gone wrong handlind request: " + exchange.getRequest().getId(), error);
        }
        if(error instanceof DuplicateKeyException){
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
        if(error instanceof ClientExceptionNoBody){
            return ResponseEntity.status(((ClientExceptionNoBody) error).getHttpStatus()).build();
        }
        else {
            ErrorDTO errorDTO;
            HttpStatus httpStatus;
            if (error instanceof ClientExceptionWithBody){
                httpStatus=((ClientExceptionWithBody) error).getHttpStatus();
                errorDTO = new ErrorDTO(Severity.ERROR, ((ClientExceptionWithBody) error).getTitle(),  error.getMessage());
            }
            else if (error instanceof ResponseStatusException) {
                httpStatus=((ResponseStatusException) error).getStatus();
                errorDTO = new ErrorDTO(Severity.ERROR, "Error",  error.getMessage());
            }
            else if(error instanceof NotEnoughFiltersException) {
                httpStatus=HttpStatus.BAD_REQUEST;
                errorDTO = new ErrorDTO(Severity.ERROR, "Error",  error.getMessage());
            }
            else {
                httpStatus=HttpStatus.INTERNAL_SERVER_ERROR;
                errorDTO = defaultErrorDTO;
            }
            return ResponseEntity.status(httpStatus)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(errorDTO);
        }
    }

}
