package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import it.gov.pagopa.idpay.transactions.model.RefundInfo;
import it.gov.pagopa.idpay.transactions.model.Reward;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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
    private List<String> initiatives;
    private Map<String, Reward> rewards;

    private String userId;
    private String maskedPan;
    private String brandLogo;

    private String operationTypeTranscoded;
    private Long effectiveAmountCents;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime trxChargeDate;
    private RefundInfo refundInfo;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime elaborationDateTime;
    private String channel;
}
