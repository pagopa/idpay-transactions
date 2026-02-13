package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportDTO {

    String id;
    String initiativeId;
    ReportStatus reportStatus;
    LocalDateTime startPeriod;
    LocalDateTime endPeriod;
    String merchantId;
    String businessName;
    LocalDateTime requestDate;
    LocalDateTime elaborationDate;
    RewardBatchAssignee operatorLevel;
    String fileName;
    ReportType reportType;

}
