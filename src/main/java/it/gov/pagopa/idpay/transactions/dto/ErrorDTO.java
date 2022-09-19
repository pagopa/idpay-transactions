package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import it.gov.pagopa.idpay.transactions.exception.Severity;
import lombok.Value;

import javax.validation.constraints.NotBlank;
@JsonInclude(JsonInclude.Include.NON_NULL)
@Value
public class ErrorDTO {
    @NotBlank
    Severity severity;
    @NotBlank
    String title;
    @NotBlank
    String message;
}
