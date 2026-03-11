package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.gov.pagopa.common.web.dto.ErrorDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class InvitaliaOutcomeResponseDTO {

    private String timestamp;

    private String code;

    private String message;

    private ErogazioneOutcomeDTO erogazione;

    private List<ErrorDTO> errors;

}