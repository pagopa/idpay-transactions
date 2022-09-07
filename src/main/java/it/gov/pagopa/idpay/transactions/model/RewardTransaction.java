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
@EqualsAndHashCode(of = {"idTrxAcquirer","acquirerCode", "trxDate", "operationType", "acquirerId"}, callSuper = false)
@Document(collection = "transaction")
public class RewardTransaction {

    @Id
    String id;

    @Field("id_trx_acquirer_s")
    String idTrxAcquirer;

    @Field("acquirer_c")
    String acquirerCode;

    @Field("trx_timestamp_t")
    LocalDateTime trxDate;

    @Field("hpan_s")
    String hpan;

    @Field("operation_type_c")
    String operationType;

    @Field("circuit_type_c")
    String circuitType;

    @Field("id_trx_issuer_s")
    String idTrxIssuer;

    @Field("correlation_id_s")
    String correlationId;

    @Field("amount_i")
    BigDecimal amount;

    @Field("amount_currency_c")
    String amountCurrency;

    @Field("mcc_c")
    String mcc;

    @Field("acquirer_id_s")
    String acquirerId;

    @Field("merchant_id_s")
    String merchantId;

    @Field("terminal_id_s")
    String terminalId;

    @Field("bin_s")
    String bin;

    @Field("sender_code_s")
    String senderCode;

    @Field("fiscal_code_s")
    String fiscalCode;

    @Field("vat_s")
    String vat;

    @Field("pos_type_s")
    String posType;

    @Field("par_s")
    String par;

    @Field("status_s")
    String status;

    @Field("rejection_reason_s")
    List<String> rejectionReasons;

    @Field("initiative_rejection_reasons_s")
    Map<String, List<String>> initiativeRejectionReasons;

    @Field("initiatives_s")
    List<String> initiatives;

    @Field("rewards_i")
    Map<String,BigDecimal> rewards;
}
