package it.gov.pagopa.idpay.transactions.model;

import it.gov.pagopa.idpay.transactions.model.counters.RewardCounters;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Reward {
    private String initiativeId;
    private String organizationId;

    /** The ruleEngine reward calculated */
    private BigDecimal providedReward;
    /** The effective reward after CAP evaluation */
    private BigDecimal accruedReward;
    /** True, if the reward has been capped due to budget threshold */
    private boolean capped;

    /** True, if the reward has been capped due to daily threshold */
    private boolean dailyCapped;
    /** True, if the reward has been capped due to monthly threshold */
    private boolean monthlyCapped;
    /** True, if the reward has been capped due to yearly threshold */
    private boolean yearlyCapped;
    /** True, if the reward has been capped due to weekly threshold */
    private boolean weeklyCapped;

    /** True if it's a refunding reward */
    private boolean refund;
    /** True if it's a complete refunding reward */
    private boolean completeRefund;

    /** Counters */
    private RewardCounters counters;
}
