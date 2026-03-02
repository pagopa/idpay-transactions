package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RefundInfo;
import it.gov.pagopa.idpay.transactions.model.Reward;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RewardTransactionDTO {

    @JsonAlias("_id")
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
    private List<String> initiatives;
    private Map<String, Reward> rewards;

    private String userId;
    private String maskedPan;
    private String brandLogo;

    private String operationTypeTranscoded;
    private Long effectiveAmountCents;
    
    private LocalDateTime trxChargeDate;
    private RefundInfo refundInfo;

    
    private LocalDateTime elaborationDateTime;
    private String channel;
    private Map<String, String> additionalProperties;
    private InvoiceData invoiceData;
    private InvoiceData creditNoteData;
    private String trxCode;
    private String franchiseName;
    private PosType pointOfSaleType;
    private String businessName;
    
    private LocalDateTime updateDate;

    private Boolean extendedAuthorization;
    private Long voucherAmountCents;

    private ChecksError checksError;

}
