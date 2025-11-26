package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MerchantTransactionDTO {
    String trxId;
    String fiscalCode;
    Long effectiveAmountCents;
    Long rewardAmountCents;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    LocalDateTime trxDate;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    @JsonProperty("updateDate")
    LocalDateTime elaborationDateTime;
    String status;
    String channel;
    String pointOfSaleId;

    LocalDateTime trxChargeDate;
    Map<String, String> additionalProperties;
    String trxCode;
    Long authorizedAmountCents;
    String docNumber;
    String fileName;
    RewardBatchTrxStatus rewardBatchTrxStatus;
}
