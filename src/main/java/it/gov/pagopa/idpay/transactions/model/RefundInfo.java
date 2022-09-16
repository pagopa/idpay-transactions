package it.gov.pagopa.idpay.transactions.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RefundInfo {
    private List<TransactionProcessed> previousTrxs;
    private Map<String, BigDecimal> previousRewards;
}
