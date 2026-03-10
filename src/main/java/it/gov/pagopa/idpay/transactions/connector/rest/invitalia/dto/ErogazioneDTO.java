package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErogazioneDTO {

    @JsonProperty("Stato")
    private String status;

    @JsonProperty("ImportoErogato")
    private BigDecimal amountPaid;

    @JsonProperty("DataValuta")
    private LocalDate dateValue;

}