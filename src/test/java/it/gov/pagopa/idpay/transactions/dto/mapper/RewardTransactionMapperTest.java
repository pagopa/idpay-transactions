package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionDTO;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionDTOFaker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;

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

    private void assertCommonFields(RewardTransaction result, RewardTransactionDTO dto) {
        Assertions.assertEquals(dto.getId() == null ? result.getId() : dto.getId(), result.getId());
        Assertions.assertEquals(dto.getIdTrxAcquirer(), result.getIdTrxAcquirer());
        Assertions.assertEquals(dto.getAcquirerCode(), result.getAcquirerCode());

        Assertions.assertEquals(
                dto.getTrxDate().atZoneSameInstant(ZoneId.of("Europe/Rome")).toLocalDateTime(),
                result.getTrxDate()
        );

        Assertions.assertEquals(dto.getHpan(), result.getHpan());
        Assertions.assertEquals(dto.getOperationType(), result.getOperationType());
        Assertions.assertEquals(dto.getCircuitType(), result.getCircuitType());
        Assertions.assertEquals(dto.getIdTrxIssuer(), result.getIdTrxIssuer());
        Assertions.assertEquals(dto.getCorrelationId(), result.getCorrelationId());
        Assertions.assertEquals(dto.getAmountCents(), result.getAmountCents());
        Assertions.assertEquals(dto.getAmountCurrency(), result.getAmountCurrency());
        Assertions.assertEquals(dto.getMcc(), result.getMcc());
        Assertions.assertEquals(dto.getAcquirerId(), result.getAcquirerId());
        Assertions.assertEquals(dto.getMerchantId(), result.getMerchantId());
        Assertions.assertEquals(dto.getPointOfSaleId(), result.getPointOfSaleId());
        Assertions.assertEquals(dto.getTerminalId(), result.getTerminalId());
        Assertions.assertEquals(dto.getBin(), result.getBin());
        Assertions.assertEquals(dto.getSenderCode(), result.getSenderCode());
        Assertions.assertEquals(dto.getFiscalCode(), result.getFiscalCode());
        Assertions.assertEquals(dto.getVat(), result.getVat());
        Assertions.assertEquals(dto.getPosType(), result.getPosType());
        Assertions.assertEquals(dto.getPar(), result.getPar());
        Assertions.assertEquals(dto.getStatus(), result.getStatus());
        Assertions.assertEquals(dto.getRejectionReasons(), result.getRejectionReasons());
        Assertions.assertEquals(dto.getInitiativeRejectionReasons(), result.getInitiativeRejectionReasons());
        Assertions.assertEquals(dto.getInitiatives(), result.getInitiatives());
        Assertions.assertEquals(dto.getRewards(), result.getRewards());
        Assertions.assertEquals(dto.getUserId(), result.getUserId());
        Assertions.assertEquals(dto.getMaskedPan(), result.getMaskedPan());
        Assertions.assertEquals(dto.getBrandLogo(), result.getBrandLogo());
        Assertions.assertEquals(dto.getOperationTypeTranscoded(), result.getOperationTypeTranscoded());
        Assertions.assertEquals(dto.getEffectiveAmountCents(), result.getEffectiveAmountCents());

        OffsetDateTime dtoChargeDate = dto.getTrxChargeDate();
        if (dtoChargeDate == null) {
            Assertions.assertNull(result.getTrxChargeDate());
        } else {
            Assertions.assertEquals(
                    dtoChargeDate.atZoneSameInstant(ZoneId.of("Europe/Rome")).toLocalDateTime(),
                    result.getTrxChargeDate()
            );
        }

        Assertions.assertEquals(dto.getRefundInfo(), result.getRefundInfo());
        Assertions.assertEquals(dto.getElaborationDateTime(), result.getElaborationDateTime());
        Assertions.assertEquals(dto.getChannel(), result.getChannel());
        Assertions.assertEquals(dto.getAdditionalProperties(), result.getAdditionalProperties());
        Assertions.assertEquals(dto.getInvoiceData(), result.getInvoiceData());
        Assertions.assertEquals(dto.getCreditNoteData(), result.getCreditNoteData());
        Assertions.assertEquals(dto.getTrxCode(), result.getTrxCode());
    }

    private void assertRefundFields(RewardTransaction result, RewardTransactionDTO dto) {
        Assertions.assertEquals(dto.getOperationTypeTranscoded(), result.getOperationTypeTranscoded());
        Assertions.assertEquals(dto.getEffectiveAmountCents(), result.getEffectiveAmountCents());
        Assertions.assertEquals(dto.getRefundInfo(), result.getRefundInfo());
        Assertions.assertEquals(dto.getRewards(), result.getRewards());
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
