package it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
@Data
@Builder
@AllArgsConstructor // <-- Questa genera il costruttore con la lista
@NoArgsConstructor
public class InstitutionList {
    List<InstitutionDTO> institutions;
}
