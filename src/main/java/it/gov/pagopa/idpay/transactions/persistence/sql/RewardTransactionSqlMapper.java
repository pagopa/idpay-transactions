package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.postgresql.codec.Json;
import it.gov.pagopa.idpay.transactions.dto.InvoiceData;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
import it.gov.pagopa.idpay.transactions.model.RefundInfo;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.records.RewardTransactionsRecord;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.jooq.JSONB;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class RewardTransactionSqlMapper {

    private final ObjectMapper objectMapper;

    RewardTransactionEntity toEntity(RewardTransaction transaction) {
        String initiativeId = initiativeId(transaction);
        return new RewardTransactionEntity(
                transaction.getId(),
                initiativeId,
                transaction.getRewardBatchId(),
                transaction.getIdTrxAcquirer(),
                transaction.getAcquirerCode(),
                transaction.getTrxDate(),
                transaction.getOperationType(),
                transaction.getCircuitType(),
                transaction.getIdTrxIssuer(),
                transaction.getCorrelationId(),
                transaction.getAmountCents(),
                transaction.getAmountCurrency(),
                transaction.getAcquirerId(),
                transaction.getMerchantId(),
                transaction.getPointOfSaleId(),
                transaction.getPosType(),
                transaction.getStatus(),
                toJson(transaction.getRejectionReasons()),
                toJson(transaction.getInitiativeRejectionReasons()),
                toJson(transaction.getRewards()),
                transaction.getUserId(),
                transaction.getOperationTypeTranscoded(),
                transaction.getEffectiveAmountCents(),
                transaction.getTrxChargeDate(),
                toJson(transaction.getRefundInfo()),
                transaction.getElaborationDateTime(),
                transaction.getChannel(),
                toJson(transaction.getAdditionalProperties()),
                toJson(transaction.getInvoiceData()),
                toJson(transaction.getCreditNoteData()),
                transaction.getTrxCode(),
                enumName(transaction.getRewardBatchTrxStatus()),
                toJson(transaction.getRewardBatchRejectionReason()),
                transaction.getRewardBatchInclusionDate(),
                transaction.getFranchiseName(),
                enumName(transaction.getPointOfSaleType()),
                transaction.getBusinessName(),
                transaction.getInvoiceUploadDate(),
                transaction.getSamplingKey(),
                transaction.getUpdateDate(),
                transaction.getExtendedAuthorization(),
                transaction.getVoucherAmountCents(),
                transaction.getRewardBatchLastMonthElaborated(),
                toJson(transaction.getChecksError()),
                accruedRewardCents(transaction.getRewards(), initiativeId)
        );
    }

    RewardTransaction fromEntity(RewardTransactionEntity entity) {
        return RewardTransaction.builder()
                .id(entity.id())
                .idTrxAcquirer(entity.idTrxAcquirer())
                .acquirerCode(entity.acquirerCode())
                .trxDate(entity.trxDate())
                .operationType(entity.operationType())
                .circuitType(entity.circuitType())
                .idTrxIssuer(entity.idTrxIssuer())
                .correlationId(entity.correlationId())
                .amountCents(entity.amountCents())
                .amountCurrency(entity.amountCurrency())
                .acquirerId(entity.acquirerId())
                .merchantId(entity.merchantId())
                .pointOfSaleId(entity.pointOfSaleId())
                .posType(entity.posType())
                .status(entity.status())
                .rejectionReasons(fromJson(entity.rejectionReasons(), new TypeReference<List<String>>() { }))
                .initiativeRejectionReasons(fromJson(entity.initiativeRejectionReasons(),
                        new TypeReference<Map<String, List<String>>>() { }))
                .initiatives(List.of(entity.initiativeId()))
                .rewards(fromJson(entity.rewards(), new TypeReference<Map<String, Reward>>() { }))
                .userId(entity.userId())
                .operationTypeTranscoded(entity.operationTypeTranscoded())
                .effectiveAmountCents(entity.effectiveAmountCents())
                .trxChargeDate(entity.trxChargeDate())
                .refundInfo(fromJson(entity.refundInfo(), RefundInfo.class))
                .elaborationDateTime(entity.elaborationDateTime())
                .channel(entity.channel())
                .additionalProperties(fromJson(entity.additionalProperties(),
                        new TypeReference<Map<String, String>>() { }))
                .invoiceData(fromJson(entity.invoiceData(), InvoiceData.class))
                .creditNoteData(fromJson(entity.creditNoteData(), InvoiceData.class))
                .trxCode(entity.trxCode())
                .rewardBatchId(entity.rewardBatchId())
                .rewardBatchTrxStatus(enumValue(entity.rewardBatchTrxStatus(), RewardBatchTrxStatus::valueOf))
                .rewardBatchRejectionReason(fromJson(entity.rewardBatchRejectionReasons(),
                        new TypeReference<List<ReasonDTO>>() { }))
                .rewardBatchInclusionDate(entity.rewardBatchInclusionDate())
                .franchiseName(entity.franchiseName())
                .pointOfSaleType(enumValue(entity.pointOfSaleType(), PosType::valueOf))
                .businessName(entity.businessName())
                .invoiceUploadDate(entity.invoiceUploadDate())
                .samplingKey(entity.samplingKey())
                .updateDate(entity.updateDate())
                .extendedAuthorization(entity.extendedAuthorization())
                .voucherAmountCents(entity.voucherAmountCents())
                .rewardBatchLastMonthElaborated(entity.rewardBatchLastMonthElaborated())
                .checksError(fromJson(entity.checksError(), ChecksError.class))
                .build();
    }

    RewardTransaction fromRecord(RewardTransactionsRecord transactionRecord) {
        return fromEntity(new RewardTransactionEntity(
                transactionRecord.getTransactionId(),
                transactionRecord.getInitiativeId(),
                transactionRecord.getRewardBatchId(),
                transactionRecord.getIdTrxAcquirer(),
                transactionRecord.getAcquirerCode(),
                transactionRecord.getTrxDate(),
                transactionRecord.getOperationType(),
                transactionRecord.getCircuitType(),
                transactionRecord.getIdTrxIssuer(),
                transactionRecord.getCorrelationId(),
                transactionRecord.getAmountCents(),
                transactionRecord.getAmountCurrency(),
                transactionRecord.getAcquirerId(),
                transactionRecord.getMerchantId(),
                transactionRecord.getPointOfSaleId(),
                transactionRecord.getPosType(),
                transactionRecord.getStatus(),
                r2dbcJson(transactionRecord.getRejectionReasons()),
                r2dbcJson(transactionRecord.getInitiativeRejectionReasons()),
                r2dbcJson(transactionRecord.getRewards()),
                transactionRecord.getUserId(),
                transactionRecord.getOperationTypeTranscoded(),
                transactionRecord.getEffectiveAmountCents(),
                transactionRecord.getTrxChargeDate(),
                r2dbcJson(transactionRecord.getRefundInfo()),
                transactionRecord.getElaborationDateTime(),
                transactionRecord.getChannel(),
                r2dbcJson(transactionRecord.getAdditionalProperties()),
                r2dbcJson(transactionRecord.getInvoiceData()),
                r2dbcJson(transactionRecord.getCreditNoteData()),
                transactionRecord.getTrxCode(),
                transactionRecord.getRewardBatchTrxStatus(),
                r2dbcJson(transactionRecord.getRewardBatchRejectionReasons()),
                transactionRecord.getRewardBatchInclusionDate(),
                transactionRecord.getFranchiseName(),
                transactionRecord.getPointOfSaleType(),
                transactionRecord.getBusinessName(),
                transactionRecord.getInvoiceUploadDate(),
                transactionRecord.getSamplingKey(),
                transactionRecord.getUpdateDate(),
                transactionRecord.getExtendedAuthorization(),
                transactionRecord.getVoucherAmountCents(),
                transactionRecord.getRewardBatchLastMonthElaborated(),
                r2dbcJson(transactionRecord.getChecksError()),
                transactionRecord.getAccruedRewardCents()
        ));
    }

    RewardTransactionsRecord toRecord(RewardTransactionEntity entity) {
        return new RewardTransactionsRecord(
                entity.id(),
                entity.initiativeId(),
                entity.rewardBatchId(),
                entity.idTrxAcquirer(),
                entity.acquirerCode(),
                entity.trxDate(),
                entity.operationType(),
                entity.circuitType(),
                entity.idTrxIssuer(),
                entity.correlationId(),
                entity.amountCents(),
                entity.amountCurrency(),
                entity.acquirerId(),
                entity.merchantId(),
                entity.pointOfSaleId(),
                entity.posType(),
                entity.status(),
                jsonb(entity.rejectionReasons()),
                jsonb(entity.initiativeRejectionReasons()),
                jsonb(entity.rewards()),
                entity.userId(),
                entity.operationTypeTranscoded(),
                entity.effectiveAmountCents(),
                entity.trxChargeDate(),
                jsonb(entity.refundInfo()),
                entity.elaborationDateTime(),
                entity.channel(),
                jsonb(entity.additionalProperties()),
                jsonb(entity.invoiceData()),
                jsonb(entity.creditNoteData()),
                entity.trxCode(),
                entity.rewardBatchTrxStatus(),
                jsonb(entity.rewardBatchRejectionReasons()),
                entity.rewardBatchInclusionDate(),
                entity.franchiseName(),
                entity.pointOfSaleType(),
                entity.businessName(),
                entity.invoiceUploadDate(),
                entity.samplingKey(),
                entity.updateDate(),
                entity.extendedAuthorization(),
                entity.voucherAmountCents(),
                entity.rewardBatchLastMonthElaborated(),
                jsonb(entity.checksError()),
                entity.accruedRewardCents()
        );
    }

    private String initiativeId(RewardTransaction transaction) {
        List<String> initiatives = transaction.getInitiatives();
        if (initiatives == null || initiatives.size() != 1 || initiatives.getFirst().isBlank()) {
            throw new IllegalArgumentException("A reward transaction must have exactly one initiative");
        }
        return initiatives.getFirst();
    }

    private long accruedRewardCents(Map<String, Reward> rewards, String initiativeId) {
        if (rewards == null || rewards.get(initiativeId) == null
                || rewards.get(initiativeId).getAccruedRewardCents() == null) {
            return 0L;
        }
        return rewards.get(initiativeId).getAccruedRewardCents();
    }

    private Json toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return Json.of(objectMapper.writeValueAsString(value));
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Unable to serialize reward transaction JSON", exception);
        }
    }

    JSONB toJsonb(Object value) {
        return jsonb(toJson(value));
    }

    private <T> T fromJson(Json value, Class<T> type) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.readValue(value.asString(), type);
        } catch (JacksonException exception) {
            throw new IllegalStateException("Unable to deserialize reward transaction JSON", exception);
        }
    }

    private <T> T fromJson(Json value, TypeReference<T> type) {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.readValue(value.asString(), type);
        } catch (JacksonException exception) {
            throw new IllegalStateException("Unable to deserialize reward transaction JSON", exception);
        }
    }

    private String enumName(Enum<?> value) {
        return value == null ? null : value.name();
    }

    private JSONB jsonb(Json value) {
        return value == null ? null : JSONB.jsonb(value.asString());
    }

    private Json r2dbcJson(JSONB value) {
        return value == null ? null : Json.of(value.data());
    }

    private <T extends Enum<T>> T enumValue(String value, java.util.function.Function<String, T> factory) {
        return value == null ? null : factory.apply(value);
    }
}
