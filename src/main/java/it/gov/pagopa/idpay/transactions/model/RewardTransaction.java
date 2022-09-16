package it.gov.pagopa.idpay.transactions.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode(of = {"id"}, callSuper = false)
@Document(collection = "transaction")
public class RewardTransaction {

    @Id
    private String id;

    @Field("id_trx_acquirer_s")
    private String idTrxAcquirer;

    @Field("acquirer_c")
    private String acquirerCode;

    @Field("trx_timestamp_t")
    private LocalDateTime trxDate;

    @Field("hpan_s")
    private String hpan;

    @Field("operation_type_c")
    private String operationType;

    @Field("circuit_type_c")
    private String circuitType;

    @Field("id_trx_issuer_s")
    private String idTrxIssuer;

    @Field("correlation_id_s")
    private String correlationId;

    @Field("amount_i")
    private BigDecimal amount;

    @Field("amount_currency_c")
    private String amountCurrency;

    @Field("mcc_c")
    private String mcc;

    @Field("acquirer_id_s")
    private String acquirerId;

    @Field("merchant_id_s")
    private String merchantId;

    @Field("terminal_id_s")
    private String terminalId;

    @Field("bin_s")
    private String bin;

    @Field("sender_code_s")
    private String senderCode;

    @Field("fiscal_code_s")
    private String fiscalCode;

    @Field("vat_s")
    private String vat;

    @Field("pos_type_s")
    private String posType;

    @Field("par_s")
    private String par;

    @Field("status_s")
    private String status;

    @Field("rejection_reason_s")
    private List<String> rejectionReasons;

    @Field("initiative_rejection_reasons_s")
    private Map<String, List<String>> initiativeRejectionReasons;

    @Field("initiatives_s")
    private List<String> initiatives;

    @Field("rewards_i")
    private Map<String,Reward> rewards;

    private String userId;

    private String operationTypeTranscoded;
    private BigDecimal effectiveAmount;
    private LocalDateTime trxChargeDate;
    private RefundInfo refundInfo;
}
