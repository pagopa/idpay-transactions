package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class PointOfSaleTransactionMapper {

  private final UserRestClient userRestClient;

  public PointOfSaleTransactionMapper(UserRestClient userRestClient) {
    this.userRestClient = userRestClient;
  }

  public Mono<PointOfSaleTransactionDTO> toDTO(RewardTransaction trx, String initiativeId, String fiscalCode) {
    PointOfSaleTransactionDTO dto = PointOfSaleTransactionDTO.builder()
        .trxId(trx.getId())
        .effectiveAmountCents(trx.getAmountCents())
        .rewardAmountCents(trx.getRewards().get(initiativeId).getAccruedRewardCents())
        .trxDate(trx.getTrxDate())
        .elaborationDateTime(trx.getElaborationDateTime())
        .status(trx.getStatus())
        .channel(trx.getChannel())
        .fiscalCode(fiscalCode)
        .additionalProperties(trx.getAdditionalProperties())
        .build();

    if (StringUtils.isNotBlank(fiscalCode)){
      dto.setFiscalCode(fiscalCode);
      return Mono.just(dto);
    } else {
      return userRestClient.retrieveUserInfo(trx.getUserId())
          .map(UserInfoPDV::getPii)
          .doOnNext(dto::setFiscalCode)
          .then(Mono.just(dto));
    }
  }
}
