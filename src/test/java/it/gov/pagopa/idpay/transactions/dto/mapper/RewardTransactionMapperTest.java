package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.common.utils.TestUtils;
import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionDTOFaker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;

@Disabled
class RewardTransactionMapperTest {

    private final RewardTransactionMapper rewardTransactionMapper = new RewardTransactionMapper();

    @Test
    void mapFromDTO_nullInput_shouldReturnNull() {
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(null);

        Assertions.assertNull(result);
    }

    @Test
    void mapFromDTO_refundTransaction_shouldMapAllMainFields() {
        RewardTransactionDTO refundTrx = RewardTransactionDTOFaker.mockInstanceRefund(1);

        RewardTransaction result = rewardTransactionMapper.mapFromDTO(refundTrx);

        Assertions.assertNotNull(result);
        assertCommonFields(result, refundTrx);
        assertRefundFields(result, refundTrx);
        assertRewardsMapped(result.getRewards());
    }

    @Test
    void mapFromDTO_rejectedTransaction_shouldMapRejectionFields() {
        RewardTransactionDTO rejectedTrx = RewardTransactionDTOFaker.mockInstanceRejected(2);

        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rejectedTrx);

        Assertions.assertNotNull(result);
        assertCommonFields(result, rejectedTrx);
        assertRejectedFields(result, rejectedTrx);
    }

    @Test
    void mapFromDTO_whenIdIsNull_shouldGenerateId() {
        RewardTransactionDTO dto = RewardTransactionDTOFaker.mockInstanceRefund(3);
        dto.setId(null);

        RewardTransaction result = rewardTransactionMapper.mapFromDTO(dto);

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.getId());

        String expectedId = dto.getIdTrxAcquirer()
                .concat(dto.getAcquirerCode())
                .concat(String.valueOf(
                        dto.getTrxDate()
                                .atZoneSameInstant(ZoneId.of("Europe/Rome"))
                                .toLocalDateTime()
                ))
                .concat(dto.getOperationType())
                .concat(dto.getAcquirerId());

        Assertions.assertEquals(expectedId, result.getId());
    }

    @Test
    void mapFromDTOTransactionWithRefund() {

        RewardTransactionDTO rewardTrx = RewardTransactionDTOFaker.mockInstanceRefund(1);

        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rewardTrx);

        Assertions.assertNotNull(result);
        assertCommonFields(result, rewardTrx);

        TestUtils.checkNotNullFields(result, "rejectionReasons", "initiativeRejectionReasons", "additionalProperties", "invoiceData", "creditNoteData", "trxCode");
        checkNotNullRewardField(result.getRewards());
        TestUtils.checkNotNullFields(result.getRefundInfo());
    }

    private void checkNotNullRewardField(Map<String, Reward> rewardMap) {
        rewardMap.forEach(
                (s,r) -> {
                    Assertions.assertNotNull(s);
                    Assertions.assertNotNull(r);
                    TestUtils.checkNotNullFields(r);
        });
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
        Assertions.assertEquals(result.getAmountCents(), rewardTrx.getAmountCents());
        Assertions.assertSame(result.getAmountCents(), rewardTrx.getAmountCents());
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
        Assertions.assertSame(result.getInitiatives(), rewardTrx.getInitiatives());
        Assertions.assertSame(result.getRewards(), rewardTrx.getRewards());
        Assertions.assertSame(result.getUserId(), rewardTrx.getUserId());
        Assertions.assertSame(result.getMaskedPan(), rewardTrx.getMaskedPan());
        Assertions.assertSame(result.getBrandLogo(), rewardTrx.getBrandLogo());
        Assertions.assertSame(result.getChannel(), rewardTrx.getChannel());
    }

    private void assertRefundFields(RewardTransaction resultRefunded, RewardTransactionDTO refundedTrx) {
        Assertions.assertSame(resultRefunded.getOperationTypeTranscoded(), refundedTrx.getOperationTypeTranscoded());
        Assertions.assertSame(resultRefunded.getEffectiveAmountCents(), refundedTrx.getEffectiveAmountCents());
        Assertions.assertEquals(resultRefunded.getTrxChargeDate(), refundedTrx.getTrxChargeDate().toLocalDateTime());
        Assertions.assertSame(resultRefunded.getRefundInfo(), refundedTrx.getRefundInfo());
        Assertions.assertSame(resultRefunded.getRewards(), refundedTrx.getRewards());
        checkNotNullRewardField(resultRefunded.getRewards());

    }

    private void assertRejectedFields(RewardTransaction result, RewardTransactionDTO dto) {
        Assertions.assertEquals(dto.getRejectionReasons(), result.getRejectionReasons());
        Assertions.assertEquals(dto.getInitiativeRejectionReasons(), result.getInitiativeRejectionReasons());
    }

    private void assertRewardsMapped(Map<String, Reward> rewards) {
        Assertions.assertNotNull(rewards);
        Assertions.assertFalse(rewards.isEmpty());

        rewards.forEach((initiativeId, reward) -> {
            Assertions.assertNotNull(initiativeId);
            Assertions.assertNotNull(reward);
            Assertions.assertNotNull(reward.getInitiativeId());
            Assertions.assertNotNull(reward.getOrganizationId());
            Assertions.assertNotNull(reward.getAccruedRewardCents());
        });
    }
}
