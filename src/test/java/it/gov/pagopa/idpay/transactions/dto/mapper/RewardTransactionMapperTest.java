package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionDTOFaker;
import it.gov.pagopa.common.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;

@Disabled
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

        RewardTransactionDTO refundTrx = RewardTransactionDTOFaker.mockInstanceRefund(1);
        RewardTransactionDTO rejectedTrx = RewardTransactionDTOFaker.mockInstanceRejected(2);

        //When
        RewardTransaction resultRefund = rewardTransactionMapper.mapFromDTO(refundTrx);
        RewardTransaction resultRejected = rewardTransactionMapper.mapFromDTO(rejectedTrx);

        //Then
        Assertions.assertNotNull(resultRefund);
        assertCommonFields(resultRefund, refundTrx);
        checkNotNullRewardField(resultRefund.getRewards());
        assertRefundFields(resultRefund,refundTrx);
        TestUtils.checkNotNullFields(resultRefund, "rejectionReasons", "initiativeRejectionReasons", "additionalProperties", "invoiceData", "creditNoteData", "trxCode", "rewardBatchId", "rewardBatchTrxStatus", "rewardBatchRejectionReason", "rewardBatchInclusionDate", "franchiseName", "pointOfSaleType", "businessName");

        Assertions.assertNotNull(resultRejected);
        assertCommonFields(resultRejected, rejectedTrx);
        assertRejectedFields(resultRejected,rejectedTrx);
        TestUtils.checkNotNullFields(resultRejected, "initiatives","rewards", "operationTypeTranscoded", "effectiveAmountCents","trxChargeDate","refundInfo", "additionalProperties", "invoiceData", "creditNoteData", "trxCode", "rewardBatchId", "rewardBatchTrxStatus", "rewardBatchRejectionReason", "rewardBatchInclusionDate", "franchiseName", "pointOfSaleType", "businessName");


    }

    @Test
    void mapFromDTOTransactionWithoutId(){
        //Given
        RewardTransactionMapper rewardTransactionMapper = new RewardTransactionMapper();

        RewardTransactionDTO rewardTrx = RewardTransactionDTOFaker.mockInstanceRefund(1);
        // When
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rewardTrx);

        //Then
        Assertions.assertNotNull(result);
        assertCommonFields(result, rewardTrx);
        TestUtils.checkNotNullFields(result, "rejectionReasons", "initiativeRejectionReasons", "additionalProperties", "invoiceData", "creditNoteData", "trxCode", "rewardBatchId", "rewardBatchTrxStatus", "rewardBatchRejectionReason", "rewardBatchInclusionDate", "franchiseName", "pointOfSaleType", "businessName");

        String expectedId = rewardTrx.getIdTrxAcquirer()
                .concat(rewardTrx.getAcquirerCode())
                .concat(String.valueOf(rewardTrx.getTrxDate().atZoneSameInstant(ZoneId.of("Europe/Rome")).toLocalDateTime()))
                .concat(rewardTrx.getOperationType())
                .concat(rewardTrx.getAcquirerId());

        Assertions.assertEquals(expectedId, result.getId());
    }

    @Test
    void mapFromDTOTransactionWithRefund() {
        //Given
        RewardTransactionMapper rewardTransactionMapper = new RewardTransactionMapper();
        RewardTransactionDTO rewardTrx = RewardTransactionDTOFaker.mockInstanceRefund(1);

        //When
        RewardTransaction result = rewardTransactionMapper.mapFromDTO(rewardTrx);

        //Then
        Assertions.assertNotNull(result);
        assertCommonFields(result, rewardTrx);

        TestUtils.checkNotNullFields(result, "rejectionReasons", "initiativeRejectionReasons", "additionalProperties", "invoiceData", "creditNoteData", "trxCode", "rewardBatchId", "rewardBatchTrxStatus", "rewardBatchRejectionReason", "rewardBatchInclusionDate", "franchiseName", "pointOfSaleType", "businessName");
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

    private void assertRejectedFields(RewardTransaction resultRejected, RewardTransactionDTO rejectedTrx) {
        Assertions.assertSame(resultRejected.getRejectionReasons(), rejectedTrx.getRejectionReasons());
        Assertions.assertSame(resultRejected.getInitiativeRejectionReasons(), rejectedTrx.getInitiativeRejectionReasons());

    }
}