package it.gov.pagopa.idpay.transactions.dto.mapper;

import it.gov.pagopa.idpay.transactions.dto.RewardTransactionKafkaDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class RewardTransactionKafkaMapper {

    private RewardTransactionKafkaMapper() {
    }

    public static RewardTransactionKafkaDTO toDto(RewardTransaction model) {
        if (model == null) {
            return null;
        }

        return RewardTransactionKafkaDTO.builder()
                .id(model.getId())
                .idTrxAcquirer(model.getIdTrxAcquirer())
                .acquirerCode(model.getAcquirerCode())
                .trxDate(toOffset(model.getTrxDate()))
                .hpan(model.getHpan())
                .operationType(model.getOperationType())
                .circuitType(model.getCircuitType())
                .idTrxIssuer(model.getIdTrxIssuer())
                .correlationId(model.getCorrelationId())

                .amountCents(model.getAmountCents())
                .amountCurrency(model.getAmountCurrency())

                .mcc(model.getMcc())
                .acquirerId(model.getAcquirerId())
                .merchantId(model.getMerchantId())
                .pointOfSaleId(model.getPointOfSaleId())
                .terminalId(model.getTerminalId())
                .bin(model.getBin())
                .senderCode(model.getSenderCode())
                .fiscalCode(model.getFiscalCode())
                .vat(model.getVat())
                .posType(model.getPosType())
                .par(model.getPar())
                .status(model.getStatus())
                .rejectionReasons(model.getRejectionReasons())
                .initiativeRejectionReasons(model.getInitiativeRejectionReasons())
                .initiativeId(model.getInitiativeId())
                .initiatives(model.getInitiatives())
                .rewards(model.getRewards())


                .userId(model.getUserId())
                .maskedPan(model.getMaskedPan())
                .brandLogo(model.getBrandLogo())

                .operationTypeTranscoded(model.getOperationTypeTranscoded())
                .effectiveAmountCents(model.getEffectiveAmountCents())
                .trxChargeDate(toOffset(model.getTrxChargeDate()))
                .refundInfo(model.getRefundInfo())

                .elaborationDateTime(model.getElaborationDateTime())
                .channel(model.getChannel())
                .additionalProperties(model.getAdditionalProperties())
                .invoiceData(model.getInvoiceData())
                .creditNoteData(model.getCreditNoteData())
                .trxCode(model.getTrxCode())


                .franchiseName(model.getFranchiseName())
                .pointOfSaleType(model.getPointOfSaleType())
                .businessName(model.getBusinessName())

                .updateDate(model.getUpdateDate())
                .extendedAuthorization(model.getExtendedAuthorization())
                .voucherAmountCents(model.getVoucherAmountCents())
                .build();
    }

    private static OffsetDateTime toOffset(LocalDateTime ldt) {
        return ldt == null ? null : ldt.atOffset(ZoneOffset.UTC);
    }
}
