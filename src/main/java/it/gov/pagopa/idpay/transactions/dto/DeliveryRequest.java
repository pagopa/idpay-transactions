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

  String id; //rewardBatchId
  String partitaIvaCliente; //recuperare da Merchant.vatNumber Se è un CF da 16, valorizzare con 11 zeri.
  String codiceFiscaleCliente; //recuperare da Merchant.fiscalCode
  String ragioneSocialeIntestatario; //recuperare da Merchant.businessName
  String cap; //selfCare
  String indirizzo; //selfCare
  String localita; //selfCare
  String provincia; //selfCare
  String pec; //selfCare
  String idPratica; //rewardBatchId
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private LocalDateTime dataAmmissione; //approvalDate di rewardBatch
  String ibanBeneficiario;  //recuperare da Merchant.iban
  Long importo; //rewardBatch.approvedAmountCents
  String autorizzatore;  //gianluca fiorillo
  String intestatarioContoCorrente; //merchant.ibanHolder

}
