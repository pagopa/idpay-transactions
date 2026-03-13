package it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErogazioneOutcomeDTO {

    @JsonProperty("Stato")
    private String status;

    @JsonProperty("ImportoErogato")
    private BigDecimal amountPaid;

    @JsonProperty("DataValuta")
    private LocalDate dateValue;

}