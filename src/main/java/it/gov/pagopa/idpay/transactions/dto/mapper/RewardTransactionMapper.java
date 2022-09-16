package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.stereotype.Service;

import java.time.ZoneId;

@Service
public class RewardTransactionMapper {
    public RewardTransaction mapFromDTO(RewardTransactionDTO rewardTrxDto){
        RewardTransaction rewardTrx = null;

        if(rewardTrxDto!=null){
            rewardTrx = RewardTransaction.builder().build();
            rewardTrx.setId(rewardTrxDto.getIdTrxAcquirer()
                    .concat(rewardTrxDto.getAcquirerCode())
                    .concat(String.valueOf(rewardTrxDto.getTrxDate().atZoneSameInstant(ZoneId.of("Europe/Rome")).toLocalDateTime()))
                    .concat(rewardTrxDto.getOperationType())
                    .concat(rewardTrxDto.getAcquirerId()));
            rewardTrx.setIdTrxAcquirer(rewardTrxDto.getIdTrxAcquirer());
            rewardTrx.setAcquirerCode(rewardTrxDto.getAcquirerCode());
            rewardTrx.setTrxDate(rewardTrxDto.getTrxDate().atZoneSameInstant(ZoneId.of("Europe/Rome")).toLocalDateTime());
            rewardTrx.setHpan(rewardTrxDto.getHpan());
            rewardTrx.setOperationType(rewardTrxDto.getOperationType());
            rewardTrx.setCircuitType(rewardTrxDto.getCircuitType());
            rewardTrx.setIdTrxIssuer(rewardTrxDto.getIdTrxIssuer());
            rewardTrx.setCorrelationId(rewardTrxDto.getCorrelationId());
            rewardTrx.setAmount(rewardTrxDto.getAmount());
            rewardTrx.setAmountCurrency(rewardTrxDto.getAmountCurrency());
            rewardTrx.setMcc(rewardTrxDto.getMcc());
            rewardTrx.setAcquirerId(rewardTrxDto.getAcquirerId());
            rewardTrx.setMerchantId(rewardTrxDto.getMerchantId());
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
        }

        return rewardTrx;
    }
}
