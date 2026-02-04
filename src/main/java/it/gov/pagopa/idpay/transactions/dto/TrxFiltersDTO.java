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
    private String fiscalCode;
    private String status;
    private String rewardBatchId;
    private RewardBatchTrxStatus rewardBatchTrxStatus;
    private String pointOfSaleId;
    private String trxCode;
}
