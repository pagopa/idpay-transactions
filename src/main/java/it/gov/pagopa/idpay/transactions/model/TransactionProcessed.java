package it.gov.pagopa.idpay.transactions.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransactionProcessed {
    private String id;

    private String idTrxAcquirer;

    private String acquirerCode;

    private LocalDateTime trxDate;

    private String operationType;

    private String acquirerId;

    private String userId;

    private String correlationId;

    private Long amountCents;
    private BigDecimal amount;

    private Map<String, Reward> rewards;

    private BigDecimal effectiveAmount;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private LocalDateTime trxChargeDate;
    private String operationTypeTranscoded;

    private LocalDateTime timestamp;
}

