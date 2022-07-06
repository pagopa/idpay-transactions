package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.springframework.stereotype.Service;

@Service
public class RewardTransactionMapper {
    public RewardTransaction mapFromDTO(RewardTransactionDTO rewardTrxDto){
        RewardTransaction rewardTrx = null;

        if(rewardTrxDto!=null){
            rewardTrx = RewardTransaction.builder().build();
            rewardTrx.setId(rewardTrxDto.getIdTrxAcquirer()
                    .concat(rewardTrxDto.getAcquirerCode())
                    .concat(String.valueOf(rewardTrxDto.getTrxDate()))
                    .concat(rewardTrxDto.getOperationType())
                    .concat(rewardTrxDto.getAcquirerId()));
            rewardTrx.setIdTrxAcquirer(rewardTrxDto.getIdTrxAcquirer());
            rewardTrx.setAcquirerCode(rewardTrxDto.getAcquirerCode());
            rewardTrx.setTrxDate(rewardTrxDto.getTrxDate());
            rewardTrx.setHpan(rewardTrxDto.getHpan());
            rewardTrx.setOperationType(rewardTrxDto.getOperationType());
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
            rewardTrx.setStatus(rewardTrx.getStatus());
            rewardTrx.setRejectionReason(rewardTrxDto.getRejectionReason());
            rewardTrx.setInitiatives(rewardTrxDto.getInitiatives());
            rewardTrx.setRewards(rewardTrxDto.getRewards());
        }

        return rewardTrx;
    }
}
