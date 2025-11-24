package it.gov.pagopa.idpay.transactions.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.LocalDateTime;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

@Data
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RewardBatchDTO {

    String id;
    String merchantId;
    String businessName;
    String month;
    PosType posType;
    String status;
    Boolean partial;
    String name;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    LocalDateTime startDate;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    LocalDateTime endDate;
    Long totalAmountCents;
    Long numberOfTransactions;
    Long numberOfTransactionsElaborated;
    String reportPath;
    String assigneeLevel;
}