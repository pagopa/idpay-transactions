package it.gov.pagopa.idpay.transactions.dto.batch;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldNameConstants;

@FieldNameConstants
@Data
@AllArgsConstructor
@Builder
public class TrxSuspendedBatchInfo {
    private String rewardBatchId;
    private Long suspendedRewardAmountCents;
    private Long initialRewardBatchAmountCents;
}
