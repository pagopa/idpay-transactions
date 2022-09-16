package it.gov.pagopa.idpay.transactions.dto;

import it.gov.pagopa.idpay.transactions.model.RefundInfo;
import it.gov.pagopa.idpay.transactions.model.Reward;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RewardTransactionDTO {

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

    private BigDecimal amount;

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

    private Map<String, Reward> rewards;

    private String userId;

    private String operationTypeTranscoded;
    private BigDecimal effectiveAmount;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime trxChargeDate;
    private RefundInfo refundInfo;

}
