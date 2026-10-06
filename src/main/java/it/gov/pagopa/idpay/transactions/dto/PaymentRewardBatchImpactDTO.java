package it.gov.pagopa.idpay.transactions.dto;

import it.gov.pagopa.idpay.transactions.enums.PaymentRewardBatchImpactType;
import java.time.OffsetDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PaymentRewardBatchImpactDTO {

    private String eventId;
    private Integer schemaVersion;
    private PaymentRewardBatchImpactType impactType;
    private OffsetDateTime occurredAt;
    private Long transactionRevision;
    private RewardTransactionDTO transaction;
}
