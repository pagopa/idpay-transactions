package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeliveryRequest {

  String id;
  String partitaIvaCliente;
  String codiceFiscaleCliente;
  String ragioneSocialeIntestatario;
  String cap;
  String indirizzo;
  String localita;
  String provincia;
  String pec;
  String idPratica;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private LocalDateTime dataAmmissione;
  String ibanBeneficiario;
  Long importo;
  String autorizzatore;
  String intestatarioContoCorrente;

}
