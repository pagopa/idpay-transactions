package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import it.gov.pagopa.idpay.transactions.enums.MerchantReportStatus;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MerchantReportDTO {

  String id;
  String initiiativeId;
  MerchantReportStatus merchantReportStatus;
  LocalDateTime startPeriod;
  LocalDateTime endPeriod;
  String merchantId;
  String businessName;
  LocalDateTime requestDate;
  LocalDateTime elaborationDate;
  RewardBatchAssignee rewardBatchAssignee;
  String fileName;
}
