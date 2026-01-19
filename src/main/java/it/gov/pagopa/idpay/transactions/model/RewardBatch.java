package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.format.annotation.DateTimeFormat;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldNameConstants
@EqualsAndHashCode(of = {"id"}, callSuper = false)
@Document(collection = "rewards_batch")
public class RewardBatch {

    @MongoId(FieldType.STRING)
    private String id;
    private String merchantId;
    private String businessName;
    private String month;
    private PosType posType;
    private RewardBatchStatus status;
    private Boolean partial;
    private String name;
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private Long approvedAmountCents;
    private Long suspendedAmountCents;
    private Long initialAmountCents;
    private Long numberOfTransactions;
    private Long numberOfTransactionsElaborated;
    private String reportPath;
    private String filename;
    private RewardBatchAssignee assigneeLevel;
    private Long numberOfTransactionsSuspended;
    private Long numberOfTransactionsRejected;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime merchantSendDate;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime approvalDate;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime creationDate;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime updateDate;

}
