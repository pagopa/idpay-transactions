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

    public RewardTransaction mapFromDTO(RewardTransactionDTO dto) {

        if (dto == null) {
            return null;
        }

        RewardTransaction rewardTrx = RewardTransaction.builder().build();

        if (StringUtils.isEmpty(dto.getId())) {
            rewardTrx.setId(dto.getIdTrxAcquirer()
                    .concat(dto.getAcquirerCode())
                    .concat(String.valueOf(dto.getTrxDate()))
                    .concat(dto.getOperationType())
                    .concat(dto.getAcquirerId()));
        } else {
            rewardTrx.setId(dto.getId());
        }

        rewardTrx.setIdTrxAcquirer(dto.getIdTrxAcquirer());
        rewardTrx.setAcquirerCode(dto.getAcquirerCode());

        rewardTrx.setTrxDate(dto.getTrxDate() != null ? toLocalDateTime(dto.getTrxDate()) : null);
        rewardTrx.setTrxChargeDate(dto.getTrxChargeDate() != null ? toLocalDateTime(dto.getTrxChargeDate()) : null);
        rewardTrx.setElaborationDateTime( dto.getElaborationDateTime()!= null ? toLocalDateTime(dto.getElaborationDateTime()): null);
        rewardTrx.setUpdateDate(dto.getUpdateDate() != null ?  toLocalDateTime(dto.getUpdateDate()): null);

        rewardTrx.setHpan(dto.getHpan());
        rewardTrx.setOperationType(dto.getOperationType());
        rewardTrx.setCircuitType(dto.getCircuitType());
        rewardTrx.setIdTrxIssuer(dto.getIdTrxIssuer());
        rewardTrx.setCorrelationId(dto.getCorrelationId());
        rewardTrx.setAmountCents(dto.getAmountCents());
        rewardTrx.setAmountCurrency(dto.getAmountCurrency());
        rewardTrx.setMcc(dto.getMcc());
        rewardTrx.setAcquirerId(dto.getAcquirerId());
        rewardTrx.setMerchantId(dto.getMerchantId());
        rewardTrx.setPointOfSaleId(dto.getPointOfSaleId());
        rewardTrx.setTerminalId(dto.getTerminalId());
        rewardTrx.setBin(dto.getBin());
        rewardTrx.setSenderCode(dto.getSenderCode());
        rewardTrx.setFiscalCode(dto.getFiscalCode());
        rewardTrx.setVat(dto.getVat());
        rewardTrx.setPosType(dto.getPosType());
        rewardTrx.setPar(dto.getPar());
        rewardTrx.setStatus(dto.getStatus());
        rewardTrx.setRejectionReasons(dto.getRejectionReasons());
        rewardTrx.setInitiativeRejectionReasons(dto.getInitiativeRejectionReasons());
        rewardTrx.setInitiatives(dto.getInitiatives());
        rewardTrx.setRewards(dto.getRewards());
        rewardTrx.setUserId(dto.getUserId());
        rewardTrx.setMaskedPan(dto.getMaskedPan());
        rewardTrx.setBrandLogo(dto.getBrandLogo());
        rewardTrx.setOperationTypeTranscoded(dto.getOperationTypeTranscoded());
        rewardTrx.setEffectiveAmountCents(dto.getEffectiveAmountCents());
        rewardTrx.setRefundInfo(dto.getRefundInfo());
        rewardTrx.setChannel(dto.getChannel());
        rewardTrx.setAdditionalProperties(dto.getAdditionalProperties());
        rewardTrx.setInvoiceData(dto.getInvoiceData());
        rewardTrx.setCreditNoteData(dto.getCreditNoteData());
        rewardTrx.setTrxCode(dto.getTrxCode());
        rewardTrx.setFranchiseName(dto.getFranchiseName());
        rewardTrx.setPointOfSaleType(dto.getPointOfSaleType());
        rewardTrx.setBusinessName(dto.getBusinessName());

        if (SyncTrxStatus.INVOICED.name().equals(dto.getStatus())) {
            rewardTrx.setInvoiceUploadDate(dto.getUpdateDate() != null ? toLocalDateTime(dto.getUpdateDate()) : null);
        }

        rewardTrx.setExtendedAuthorization(dto.getExtendedAuthorization());
        rewardTrx.setVoucherAmountCents(dto.getVoucherAmountCents());
        rewardTrx.setChecksError(dto.getChecksError());

        return rewardTrx;
    }

    private LocalDateTime toLocalDateTime(OffsetDateTime offsetDateTime) {
        return offsetDateTime.atZoneSameInstant(ZoneId.of("Europe/Rome")).toLocalDateTime();
    }
}
