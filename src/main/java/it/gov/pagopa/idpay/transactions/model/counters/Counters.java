package it.gov.pagopa.idpay.transactions.model.counters;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class Counters {
    private Long trxNumber;
    private BigDecimal totalReward;
    private BigDecimal totalAmount;
}
