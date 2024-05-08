package it.gov.pagopa.idpay.transactions.model;

import lombok.*;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;
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
    private LocalDateTime trxDate;
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
    private List<String> initiatives;
    private Map<String,Reward> rewards;

    private String userId;
    private String maskedPan;
    private String brandLogo;

    private String operationTypeTranscoded;
    private Long effectiveAmountCents;
    private LocalDateTime trxChargeDate;
    private RefundInfo refundInfo;

    private LocalDateTime elaborationDateTime;
    private String channel;
}
