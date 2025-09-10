package it.gov.pagopa.idpay.transactions.test.fakers;

import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;

public class PointOfSaleTransactionDTOFaker {

  private PointOfSaleTransactionDTOFaker() {}

  public static PointOfSaleTransactionDTO mockInstance(RewardTransaction trx, String initiativeId, String fiscalCode) {
    return PointOfSaleTransactionDTO.builder()
        .trxId(trx.getId())
        .fiscalCode(fiscalCode != null ? fiscalCode : trx.getFiscalCode())
        .effectiveAmountCents(trx.getEffectiveAmountCents() != null ? trx.getEffectiveAmountCents() : trx.getAmountCents())
        .rewardAmountCents(
            trx.getRewards() != null && trx.getRewards().get(initiativeId) != null
                ? trx.getRewards().get(initiativeId).getAccruedRewardCents()
                : null
        )
        .trxDate(trx.getTrxDate())
        .elaborationDateTime(trx.getElaborationDateTime())
        .status(trx.getStatus())
        .channel(trx.getChannel())
        .build();
  }
}
