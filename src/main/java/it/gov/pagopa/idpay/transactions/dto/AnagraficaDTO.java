package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnagraficaDTO {

  String partitaIvaCliente;
  String codiceFiscaleCliente;
  String ragioneSocialeIntestatario;
  String pec;
  String indirizzo;
  String cap;
  String localita;
  String provincia;
}
