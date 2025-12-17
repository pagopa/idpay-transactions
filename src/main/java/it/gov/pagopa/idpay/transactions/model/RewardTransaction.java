package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import lombok.*;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldNameConstants
@EqualsAndHashCode(of = {"id"}, callSuper = false)
@Document(collection = "transaction")
public class RewardTransaction {

    @Id
    private String id;
    private String idTrxAcquirer;
    private String acquirerCode;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime trxDate;
    private String hpan;
    private String operationType;
    private String circuitType;
    private String idTrxIssuer;
    private String correlationId;

    private Long amountCents;
    private String amountCurrency;

    private String mcc;
    private String acquirerId;
    private String merchantId;
    private String pointOfSaleId;
    private String terminalId;
    private String bin;
    private String senderCode;
    private String fiscalCode;
    private String vat;
    private String posType;
    private String par;
    private String status;
    private List<String> rejectionReasons;
    private Map<String, List<String>> initiativeRejectionReasons;
    private String initiativeId;
    private List<String> initiatives;
    private Map<String,Reward> rewards;

    private String userId;
    private String maskedPan;
    private String brandLogo;

    private String operationTypeTranscoded;
    private Long effectiveAmountCents;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime trxChargeDate;
    private RefundInfo refundInfo;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime elaborationDateTime;
    private String channel;
    private Map<String, String> additionalProperties;
    private InvoiceData invoiceData;
    private InvoiceData creditNoteData;
    private String trxCode;

    private String rewardBatchId;
    private RewardBatchTrxStatus rewardBatchTrxStatus;
    private String rewardBatchRejectionReason;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime rewardBatchInclusionDate;
    private String franchiseName;
    private PosType pointOfSaleType;
    private String businessName;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime invoiceUploadDate;

    //key used for randomly select a transaction for evaluation
    private int samplingKey;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime updateDate;
    private Boolean extendedAuthorization;
    private Long voucherAmountCents;
}
