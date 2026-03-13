package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErogazioneDTO {

  String idPratica;
  @JsonFormat(
          shape = JsonFormat.Shape.STRING,
          pattern = "yyyyMMdd'T'HH:mm:ss'Z'",
          timezone = "UTC"
  )
  private LocalDateTime dataAmmissione;
  String ibanBeneficiario;
  Double importo;
  String intestatarioContoCorrente;
  String autorizzatore;

}
