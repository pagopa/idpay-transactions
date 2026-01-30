package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.connector.rest.UserRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.dto.UserInfoPDV;
import it.gov.pagopa.idpay.transactions.dto.InvoiceFile;
import it.gov.pagopa.idpay.transactions.dto.PointOfSaleTransactionDTO;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
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

  public Mono<PointOfSaleTransactionDTO> toDTO(RewardTransaction trx, String initiativeId,
      String fiscalCode) {

    Long totalAmount = trx.getAmountCents();

    Long rewardAmount = 0L;

    if (trx.getRewards() != null && trx.getRewards().get(initiativeId) != null) {
      rewardAmount = Math.abs(trx.getRewards().get(initiativeId).getAccruedRewardCents());
    }

    Long authorizedAmount = totalAmount - rewardAmount;

    InvoiceFile invoiceFile = null;

    if ((SyncTrxStatus.INVOICED.name().equalsIgnoreCase(trx.getStatus())
        || SyncTrxStatus.REWARDED.name().equalsIgnoreCase(
        trx.getStatus()))
        && trx.getInvoiceData() != null) {
      invoiceFile = InvoiceFile.builder()
          .filename(trx.getInvoiceData().getFilename())
          .docNumber(trx.getInvoiceData().getDocNumber())
          .build();
    } else if (SyncTrxStatus.REFUNDED.name().equalsIgnoreCase(trx.getStatus())
        && trx.getCreditNoteData() != null) {
      invoiceFile = InvoiceFile.builder()
          .filename(trx.getCreditNoteData().getFilename())
          .docNumber(trx.getCreditNoteData().getDocNumber())
          .build();
    }

    PointOfSaleTransactionDTO dto = PointOfSaleTransactionDTO.builder()
        .trxId(trx.getId())
            .trxCode(trx.getTrxCode())
        .effectiveAmountCents(trx.getAmountCents())
        .rewardAmountCents(rewardAmount)
        .authorizedAmountCents(authorizedAmount)
        .trxDate(trx.getTrxDate())
        .trxChargeDate(trx.getTrxChargeDate())
        .elaborationDateTime(trx.getElaborationDateTime())
        .status(trx.getStatus())
            .rewardBatchTrxStatus(
                    trx.getRewardBatchTrxStatus() != null ? trx.getRewardBatchTrxStatus().name() : null
            )
        .channel(trx.getChannel())
        .fiscalCode(fiscalCode)
        .additionalProperties(trx.getAdditionalProperties())
        .invoiceFile(invoiceFile)
        .build();

    if (StringUtils.isNotBlank(fiscalCode)) {
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
