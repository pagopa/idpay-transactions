package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import java.time.Instant;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldNameConstants
@EqualsAndHashCode(of = {"id"}, callSuper = false)
@Document(collection = "rewards_batch")
public class RewardBatch {

    @Id
    private String id;
    private String merchantId;
    private String businessName;
    private String month;
    private String posType;
    private RewardBatchStatus status;
    private Boolean partial;
    private String name;
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private Long totalAmountCents;
    private Long numberOfTransactions;
    private Long numberOfTransactionsElaborated;
    private String reportPath;

    @CreatedDate
    private Instant creationDate;

    @LastModifiedDate
    private Instant updateDate;
}
