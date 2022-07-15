package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        RewardTransactionDTO rewardTrx = RewardTransactionDTO.builder().build();
        //region initializer RewardTransactionDTO
        rewardTrx.setIdTrxAcquirer("98174002165501220007165503");
        rewardTrx.setAcquirerCode("36081");
        rewardTrx.setTrxDate(trxDate);
        rewardTrx.setHpan("5c6bda1b1f5f6238dcba70f9f4b5a77671eb2b1563b0ca6d15d14c649a9b7ce0");
        rewardTrx.setOperationType("00");
        rewardTrx.setCircuitType("01");
        rewardTrx.setIdTrxIssuer("005422");
        rewardTrx.setCorrelationId("123456789");
        rewardTrx.setAmount(new BigDecimal("100.00"));
        rewardTrx.setAmountCurrency("978");
        rewardTrx.setMcc("4900");
        rewardTrx.setAcquirerId("09509");
        rewardTrx.setMerchantId("40000098174");
        rewardTrx.setTerminalId("98174002");
        rewardTrx.setBin("54133300");
        rewardTrx.setSenderCode("54133300");
        rewardTrx.setFiscalCode("fc549921");
        rewardTrx.setVat("123456");
        rewardTrx.setPosType("01");
        rewardTrx.setPar("par1");
        rewardTrx.setStatus("ACCEPTED");
        rewardTrx.setRejectionReason(null);
        rewardTrx.setInitiatives(initiative);
        rewardTrx.setRewards(reward);
        //endregion

        //When
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rewardTrx);

        //Then
        Assertions.assertNotNull(result);
        TestUtils.checkNotNullFields(result,"rejectionReason");
    }
}