package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.annotations.ApiModelProperty;
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
    @ApiModelProperty(required = true, value = "Severity level of the error message", example = "ERROR")
    Severity severity;
    @NotBlank
    @ApiModelProperty(required = true, value = "Content of the error message", example = "Messaggio")
    String title;
    @NotBlank
    @ApiModelProperty(required = true, value = "Title of the error message", example = "Titolo")
    String message;

}
