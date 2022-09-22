package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.RefundInfo;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionDTOFaker;
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
        Map<String, Reward> reward = new HashMap<>();
        reward.put("initiativeID", new Reward());
        Map<String, List<String>> initiativeRejectionsReason = new HashMap<>();
        initiativeRejectionsReason.put("initiative", List.of("Error initiative"));

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
        rewardTrx.setRejectionReasons(List.of("ERROR"));
        rewardTrx.setInitiativeRejectionReasons(initiativeRejectionsReason);
        rewardTrx.setInitiatives(initiative);
        rewardTrx.setRewards(reward);
        rewardTrx.setUserId("UserId");
        rewardTrx.setOperationTypeTranscoded("OperationTypeTranscoded");
        rewardTrx.setEffectiveAmount(BigDecimal.TEN);
        rewardTrx.setTrxChargeDate(rewardTrx.getTrxDate().minusDays(1));
        rewardTrx.setRefundInfo(new RefundInfo());
        //endregion

        //When
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rewardTrx);

        //Then
        Assertions.assertNotNull(result);
        assertCommonFields(result, rewardTrx);

        TestUtils.checkNotNullFields(result);
    }

    @Test
    void mapFromDTOTransactionWithId(){
        //Given
        RewardTransactionMapper rewardTransactionMapper = new RewardTransactionMapper();

        List<String> initiative = List.of("initiativeId");
        Map<String, Reward> reward = new HashMap<>();
        reward.put("initiativeID", new Reward());
        Map<String, List<String>> initiativeRejectionsReason = new HashMap<>();
        initiativeRejectionsReason.put("initiative", List.of("Error initiative"));

        RewardTransactionDTO rewardTrx = RewardTransactionDTOFaker.mockInstance(1);
        rewardTrx.setId("IDPRESENTE");
        rewardTrx.setStatus("ACCEPTED");
        rewardTrx.setRejectionReasons(List.of("ERROR"));
        rewardTrx.setInitiativeRejectionReasons(initiativeRejectionsReason);
        rewardTrx.setInitiatives(initiative);
        rewardTrx.setRewards(reward);
        rewardTrx.setUserId("UserId");
        rewardTrx.setOperationTypeTranscoded("OperationTypeTranscoded");
        rewardTrx.setEffectiveAmount(BigDecimal.TEN);
        rewardTrx.setTrxChargeDate(rewardTrx.getTrxDate().minusDays(1));
        rewardTrx.setRefundInfo(new RefundInfo());

        // When
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rewardTrx);

        //Then
        Assertions.assertNotNull(result);
        assertCommonFields(result, rewardTrx);
        Assertions.assertEquals("IDPRESENTE", result.getId());

        TestUtils.checkNotNullFields(result);
    }

    private void assertCommonFields(RewardTransaction result, RewardTransactionDTO rewardTrx) {
        Assertions.assertSame(result.getIdTrxAcquirer(), rewardTrx.getIdTrxAcquirer());
        Assertions.assertSame(result.getAcquirerCode(), rewardTrx.getAcquirerCode());
        Assertions.assertEquals(result.getTrxDate(), rewardTrx.getTrxDate().toLocalDateTime());
        Assertions.assertSame(result.getHpan(), rewardTrx.getHpan());
        Assertions.assertSame(result.getOperationType(), rewardTrx.getOperationType());
        Assertions.assertSame(result.getCircuitType(), rewardTrx.getCircuitType());
        Assertions.assertSame(result.getIdTrxIssuer(), rewardTrx.getIdTrxIssuer());
        Assertions.assertSame(result.getCorrelationId(), rewardTrx.getCorrelationId());
        Assertions.assertSame(result.getAmount(), rewardTrx.getAmount());
        Assertions.assertSame(result.getAmountCurrency(), rewardTrx.getAmountCurrency());
        Assertions.assertSame(result.getMcc(), rewardTrx.getMcc());
        Assertions.assertSame(result.getAcquirerId(), rewardTrx.getAcquirerId());
        Assertions.assertSame(result.getMerchantId(), rewardTrx.getMerchantId());
        Assertions.assertSame(result.getTerminalId(), rewardTrx.getTerminalId());
        Assertions.assertSame(result.getBin(), rewardTrx.getBin());
        Assertions.assertSame(result.getSenderCode(), rewardTrx.getSenderCode());
        Assertions.assertSame(result.getFiscalCode(), rewardTrx.getFiscalCode());
        Assertions.assertSame(result.getVat(), rewardTrx.getVat());
        Assertions.assertSame(result.getPosType(), rewardTrx.getPosType());
        Assertions.assertSame(result.getPar(), rewardTrx.getPar());
        Assertions.assertSame(result.getStatus(), rewardTrx.getStatus());
        Assertions.assertSame(result.getRejectionReasons(), rewardTrx.getRejectionReasons());
        Assertions.assertSame(result.getInitiativeRejectionReasons(), rewardTrx.getInitiativeRejectionReasons());
        Assertions.assertSame(result.getInitiatives(), rewardTrx.getInitiatives());
        Assertions.assertSame(result.getRewards(), rewardTrx.getRewards());
        Assertions.assertSame(result.getUserId(), rewardTrx.getUserId());
        Assertions.assertSame(result.getOperationTypeTranscoded(), rewardTrx.getOperationTypeTranscoded());
        Assertions.assertSame(result.getEffectiveAmount(), rewardTrx.getEffectiveAmount());
        Assertions.assertEquals(result.getTrxChargeDate(), rewardTrx.getTrxChargeDate().toLocalDateTime());
        Assertions.assertSame(result.getRefundInfo(), rewardTrx.getRefundInfo());
    }
}