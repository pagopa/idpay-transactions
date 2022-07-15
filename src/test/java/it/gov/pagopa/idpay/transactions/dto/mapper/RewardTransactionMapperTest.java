package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RewardTransactionMapperTest {

    @Test
    void mapFromDTO() {
        //Given
        RewardTransactionMapper rewardTransactionMapper = new RewardTransactionMapper();

        //When
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(null);

        //Then
        Assertions.assertNull(result);
    }

    @Test
    void mapFromDTOTransaction() {
        //Given
        RewardTransactionMapper rewardTransactionMapper = new RewardTransactionMapper();
        OffsetDateTime trxDate = OffsetDateTime.now();
        List<String> initiative = List.of("initiativeId");
        Map<String,BigDecimal> reward = new HashMap<>();
        reward.put("initiativeID",new BigDecimal("30.00"));

        //region initializer RewardTransactionDTO
        RewardTransactionDTO rewardTrx = new RewardTransactionDTO("98174002165501220007165503",
                "36081",
                trxDate,
                "5c6bda1b1f5f6238dcba70f9f4b5a77671eb2b1563b0ca6d15d14c649a9b7ce0",
                "00",
                "01",
                "005422",
                "123456789",
                new BigDecimal("100.00"),
                "978",
                "4900",
                "09509",
                "40000098174",
                "98174002",
                "54133300",
                "54133300",
                "fc549921",
                "123456",
                "01",
                "par1",
                null,
                null,
                initiative,
                reward);
        //endregion

        //When
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rewardTrx);

        //Then
        Assertions.assertNotNull(result);
        Assertions.assertEquals(rewardTrx.getIdTrxAcquirer(),result.getIdTrxAcquirer());
        Assertions.assertEquals(rewardTrx.getAcquirerCode(),result.getAcquirerCode());
        Assertions.assertEquals(rewardTrx.getTrxDate().toLocalDateTime(),result.getTrxDate());

        Assertions.assertEquals(rewardTrx.getCorrelationId(),result.getCorrelationId());
        Assertions.assertEquals(rewardTrx.getCircuitType(),result.getCircuitType());
        Assertions.assertEquals(rewardTrx.getAmount(),result.getAmount());
        Assertions.assertEquals(rewardTrx.getAmountCurrency(),result.getAmountCurrency());
        Assertions.assertEquals(rewardTrx.getMcc(),result.getMcc());
        Assertions.assertEquals(rewardTrx.getAcquirerId(),result.getAcquirerId());
        Assertions.assertEquals(rewardTrx.getMerchantId(),result.getMerchantId());
        Assertions.assertEquals(rewardTrx.getTerminalId(),result.getTerminalId());
        Assertions.assertEquals(rewardTrx.getBin(),result.getBin());
        Assertions.assertEquals(rewardTrx.getSenderCode(),result.getSenderCode());
        Assertions.assertEquals(rewardTrx.getFiscalCode(),result.getFiscalCode());
        Assertions.assertEquals(rewardTrx.getVat(),result.getVat());
        Assertions.assertEquals(rewardTrx.getPosType(),result.getPosType());
        Assertions.assertEquals(rewardTrx.getPar(),result.getPar());
        Assertions.assertEquals(rewardTrx.getStatus(),result.getStatus());
        Assertions.assertEquals(rewardTrx.getRejectionReason(),result.getRejectionReason());
        Assertions.assertEquals(rewardTrx.getInitiatives(),result.getInitiatives());
        Assertions.assertEquals(rewardTrx.getRewards(),result.getRewards());
        Assertions.assertEquals(rewardTrx.getIdTrxAcquirer()
                .concat(rewardTrx.getAcquirerCode())
                .concat(String.valueOf(rewardTrx.getTrxDate().toLocalDateTime()))
                .concat(rewardTrx.getOperationType())
                .concat(rewardTrx.getAcquirerId()), result.getId());
    }
}