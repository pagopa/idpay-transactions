package it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class InstitutionDTO {
    private String description; //ragione sociale
    private String digitalAddress; //pec
    private String address; //indirizzo
    private String zipCode; //cap
    private String taxCode; //partita iva
    private String city; //localita
    private String county; //provincia
    private String country; //paese "IT"
}
