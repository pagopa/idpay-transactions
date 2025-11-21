package it.gov.pagopa.idpay.transactions.dto;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TrxFiltersDTO {

    private String merchantId;
    private String initiativeId;
    private String status;
    private String userId;
    private String rewardBatchId;
    private RewardBatchTrxStatus rewardBatchTrxStatus;
}
