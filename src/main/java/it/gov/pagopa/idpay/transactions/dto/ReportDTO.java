package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportDTO {

    String id;
    String initiativeId;
    ReportStatus reportStatus;
    Instant startPeriod;
    Instant endPeriod;
    String merchantId;
    String businessName;
    Instant requestDate;
    Instant elaborationDate;
    RewardBatchAssignee operatorLevel;
    String fileName;
    ReportType reportType;

}
