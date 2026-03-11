package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.gov.pagopa.idpay.transactions.dto.AnagraficaDTO;
import it.gov.pagopa.idpay.transactions.dto.ErogazioneDTO;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErogazioneOutcomeDTO {

    @JsonProperty("anagrafica")
    private AnagraficaDTO anagraficaDTO;

    @JsonProperty("erogazione")
    private ErogazioneDTO erogazioneDTO;

    @JsonProperty("Stato")
    private String status;

    @JsonProperty("ImportoErogato")
    private BigDecimal amountPaid;

    @JsonProperty("DataValuta")
    private LocalDate dateValue;

}