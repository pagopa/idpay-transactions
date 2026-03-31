package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class ErrorInvitaliaDTO {

    @NotBlank
    @JsonProperty("Code")
    private String code;

    @NotBlank
    @JsonProperty("Message")
    private String message;
}
