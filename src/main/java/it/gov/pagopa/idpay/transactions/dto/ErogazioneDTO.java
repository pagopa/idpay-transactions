package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErogazioneDTO {

  String idPratica;
  @JsonFormat(
          shape = JsonFormat.Shape.STRING,
          pattern = "yyyyMMdd'T'HH:mm:ssX",
          timezone = "UTC"
  )
  private Instant dataAmmissione;
  String ibanBeneficiario;
  Double importo;
  String intestatarioContoCorrente;
  String autorizzatore;

}
