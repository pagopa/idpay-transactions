package it.gov.pagopa.idpay.transactions.model.counters;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class RewardCounters extends Counters{
    private boolean exhaustedBudget;
    private Long initiativeBudgetCents;
}
