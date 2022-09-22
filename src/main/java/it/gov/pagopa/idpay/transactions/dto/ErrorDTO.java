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
    @ApiModelProperty(required = true, value = "Livello di severity del messaggio di errore", example = "ERROR")
    Severity severity;
    @NotBlank
    @ApiModelProperty(required = true, value = "Contenuto del messaggio di errore", example = "Messaggio")
    String title;
    @NotBlank
    @ApiModelProperty(required = true, value = "Titolo del messaggio di errore", example = "Titolo")
    String message;

}
