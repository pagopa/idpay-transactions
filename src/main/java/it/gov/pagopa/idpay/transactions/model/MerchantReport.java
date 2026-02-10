package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.enums.ReportStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import lombok.*;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldNameConstants
@EqualsAndHashCode(of = {"id"}, callSuper = false)
@Document(collection = "merchantReport")
public class MerchantReport {

    @Id
    private String id;
    private String initiativeId;
    private ReportStatus reportStatus;
    private LocalDateTime startPeriod;
    private LocalDateTime endPeriod;
    private String merchantId;
    private String businessName;
    private LocalDateTime requestDate;
    private LocalDateTime elaborationDate;
    private RewardBatchAssignee rewardBatchAssignee;
    private String fileName;
}
