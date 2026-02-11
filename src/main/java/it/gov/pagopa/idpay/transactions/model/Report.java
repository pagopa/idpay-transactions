package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.ReportType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldNameConstants
@EqualsAndHashCode(of = {"id"}, callSuper = false)
@Document(collection = "reports")
public class Report {

    @MongoId
    private String id;
    private String initiativeId;
    private ReportStatus reportStatus;
    private LocalDateTime startPeriod;
    private LocalDateTime endPeriod;
    private String merchantId;
    private String businessName;
    private LocalDateTime requestDate;
    private LocalDateTime elaborationDate;
    private RewardBatchAssignee operatorLevel;
    private String fileName;
    private ReportType reportType;
}
