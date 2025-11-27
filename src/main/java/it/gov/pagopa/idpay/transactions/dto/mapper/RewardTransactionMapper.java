package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;

@Service
public class RewardTransactionMapper {
    public RewardTransaction mapFromDTO(RewardTransactionDTO rewardTrxDto) {
        RewardTransaction rewardTrx = null;

        if (rewardTrxDto != null) {
            rewardTrx = RewardTransaction.builder().build();

            if (StringUtils.isEmpty(rewardTrxDto.getId())) {
                rewardTrx.setId(rewardTrxDto.getIdTrxAcquirer()
                        .concat(rewardTrxDto.getAcquirerCode())
                        .concat(String.valueOf(toLocalDateTime(rewardTrxDto.getTrxDate())))
                        .concat(rewardTrxDto.getOperationType())
                        .concat(rewardTrxDto.getAcquirerId()));
            } else {
                rewardTrx.setId(rewardTrxDto.getId());
            }
            rewardTrx.setIdTrxAcquirer(rewardTrxDto.getIdTrxAcquirer());
            rewardTrx.setAcquirerCode(rewardTrxDto.getAcquirerCode());
            rewardTrx.setTrxDate(toLocalDateTime(rewardTrxDto.getTrxDate()));
            rewardTrx.setHpan(rewardTrxDto.getHpan());
            rewardTrx.setOperationType(rewardTrxDto.getOperationType());
            rewardTrx.setCircuitType(rewardTrxDto.getCircuitType());
            rewardTrx.setIdTrxIssuer(rewardTrxDto.getIdTrxIssuer());
            rewardTrx.setCorrelationId(rewardTrxDto.getCorrelationId());
            rewardTrx.setAmountCents(rewardTrxDto.getAmountCents());
            rewardTrx.setAmountCurrency(rewardTrxDto.getAmountCurrency());
            rewardTrx.setMcc(rewardTrxDto.getMcc());
            rewardTrx.setAcquirerId(rewardTrxDto.getAcquirerId());
            rewardTrx.setMerchantId(rewardTrxDto.getMerchantId());
            rewardTrx.setPointOfSaleId(rewardTrxDto.getPointOfSaleId());
            rewardTrx.setTerminalId(rewardTrxDto.getTerminalId());
            rewardTrx.setBin(rewardTrxDto.getBin());
            rewardTrx.setSenderCode(rewardTrxDto.getSenderCode());
            rewardTrx.setFiscalCode(rewardTrxDto.getFiscalCode());
            rewardTrx.setVat(rewardTrxDto.getVat());
            rewardTrx.setPosType(rewardTrxDto.getPosType());
            rewardTrx.setPar(rewardTrxDto.getPar());
            rewardTrx.setStatus(rewardTrxDto.getStatus());
            rewardTrx.setRejectionReasons(rewardTrxDto.getRejectionReasons());
            rewardTrx.setInitiativeRejectionReasons(rewardTrxDto.getInitiativeRejectionReasons());
            rewardTrx.setInitiatives(rewardTrxDto.getInitiatives());
            rewardTrx.setRewards(rewardTrxDto.getRewards());
            rewardTrx.setUserId(rewardTrxDto.getUserId());
            rewardTrx.setMaskedPan(rewardTrxDto.getMaskedPan());
            rewardTrx.setBrandLogo(rewardTrxDto.getBrandLogo());
            rewardTrx.setOperationTypeTranscoded(rewardTrxDto.getOperationTypeTranscoded());
            rewardTrx.setEffectiveAmountCents(rewardTrxDto.getEffectiveAmountCents());
            rewardTrx.setTrxChargeDate(rewardTrxDto.getTrxChargeDate() != null ? toLocalDateTime(rewardTrxDto.getTrxChargeDate()) : null);
            rewardTrx.setRefundInfo(rewardTrxDto.getRefundInfo());

            rewardTrx.setElaborationDateTime(rewardTrxDto.getElaborationDateTime());
            rewardTrx.setChannel(rewardTrxDto.getChannel());
            rewardTrx.setAdditionalProperties(rewardTrxDto.getAdditionalProperties());
            rewardTrx.setInvoiceData(rewardTrxDto.getInvoiceData());
            rewardTrx.setCreditNoteData(rewardTrxDto.getCreditNoteData());
            rewardTrx.setTrxCode((rewardTrxDto.getTrxCode()));
            if(SyncTrxStatus.INVOICED.name().equals(rewardTrxDto.getStatus())){
              rewardTrx.setInvoiceUploadDate(rewardTrxDto.getUpdateDate());
            }


        }

        return rewardTrx;
    }

    private LocalDateTime toLocalDateTime(OffsetDateTime offsetDateTime) {
        return offsetDateTime.atZoneSameInstant(ZoneId.of("Europe/Rome")).toLocalDateTime();
    }
}
