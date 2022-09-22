package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import it.gov.pagopa.idpay.transactions.exception.Severity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class ErrorDTO {
    @NotBlank
    Severity severity;
    @NotBlank
    String title;
    @NotBlank
    String message;

}
