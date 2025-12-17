package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PointOfSaleTransactionDTO {

  @JsonProperty("id")
  String trxId;
  String fiscalCode;
  Long effectiveAmountCents;
  Long rewardAmountCents;
  Long authorizedAmountCents;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  OffsetDateTime trxDate;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  OffsetDateTime trxChargeDate;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  @JsonProperty("updateDate")
  OffsetDateTime elaborationDateTime;
  String status;
  String channel;
  Map<String, String> additionalProperties;
  InvoiceFile invoiceFile;
}
