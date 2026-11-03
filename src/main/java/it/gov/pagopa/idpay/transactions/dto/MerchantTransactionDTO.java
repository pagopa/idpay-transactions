package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MerchantTransactionDTO {
    String trxId;
    Long transactionRevision;
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
    InvoiceData invoiceData;
    RewardBatchTrxStatus rewardBatchTrxStatus;
    @Schema(description = "Internal notes. Returned only to institution operators")
    List<ReasonDTO> rewardBatchRejectionReason;
    @Schema(description = "Official notes visible to institution operators and merchants")
    List<ReasonDTO> rewardBatchOfficialNotes;
    ChecksErrorDTO checksError;
    String franchiseName;
}
