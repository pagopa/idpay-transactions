package it.gov.pagopa.idpay.transactions.connector.rest.selfcare.dto;

import lombok.Data;

import java.util.List;

@Data
public class InstitutionList {
    List<InstitutionDTO> institutions;
}
