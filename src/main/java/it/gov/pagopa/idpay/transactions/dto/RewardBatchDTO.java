package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RewardBatchDTO {

  String id;
  String merchantId;
  String initiativeId;
  String businessName;
  String month;
  PosType posType;
  String status;
  Boolean partial;
  String name;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  Instant startDate;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  Instant endDate;
  Long approvedAmountCents;
  Long suspendedAmountCents;
  Long initialAmountCents;
  Long numberOfTransactions;
  Long numberOfTransactionsSuspended;
  Long numberOfTransactionsRejected;
  Long numberOfTransactionsElaborated;
  String reportPath;
  String assigneeLevel;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  Instant merchantSendDate;

  private Instant refundValutaDate;
  private String refundErrorMessage;
  private Instant refundOutcomeTimestamp;
}
