package it.gov.pagopa.idpay.transactions.persistence.sql;

import io.r2dbc.postgresql.codec.Json;
import java.time.LocalDateTime;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("reward_transactions")
public record RewardTransactionEntity(
        @Id @Column("transaction_id") String id,
        @Column("initiative_id") String initiativeId,
        @Column("reward_batch_id") String rewardBatchId,
        @Column("id_trx_acquirer") String idTrxAcquirer,
        @Column("acquirer_code") String acquirerCode,
        @Column("trx_date") LocalDateTime trxDate,
        @Column("operation_type") String operationType,
        @Column("circuit_type") String circuitType,
        @Column("id_trx_issuer") String idTrxIssuer,
        @Column("correlation_id") String correlationId,
        @Column("amount_cents") Long amountCents,
        @Column("amount_currency") String amountCurrency,
        @Column("acquirer_id") String acquirerId,
        @Column("merchant_id") String merchantId,
        @Column("point_of_sale_id") String pointOfSaleId,
        @Column("pos_type") String posType,
        String status,
        @Column("rejection_reasons") Json rejectionReasons,
        @Column("initiative_rejection_reasons") Json initiativeRejectionReasons,
        Json rewards,
        @Column("user_id") String userId,
        @Column("operation_type_transcoded") String operationTypeTranscoded,
        @Column("effective_amount_cents") Long effectiveAmountCents,
        @Column("trx_charge_date") LocalDateTime trxChargeDate,
        @Column("refund_info") Json refundInfo,
        @Column("elaboration_date_time") LocalDateTime elaborationDateTime,
        String channel,
        @Column("additional_properties") Json additionalProperties,
        @Column("invoice_data") Json invoiceData,
        @Column("credit_note_data") Json creditNoteData,
        @Column("trx_code") String trxCode,
        @Column("reward_batch_trx_status") String rewardBatchTrxStatus,
        @Column("reward_batch_rejection_reasons") Json rewardBatchRejectionReasons,
        @Column("reward_batch_inclusion_date") LocalDateTime rewardBatchInclusionDate,
        @Column("franchise_name") String franchiseName,
        @Column("point_of_sale_type") String pointOfSaleType,
        @Column("business_name") String businessName,
        @Column("invoice_upload_date") LocalDateTime invoiceUploadDate,
        @Column("sampling_key") int samplingKey,
        @Column("update_date") LocalDateTime updateDate,
        @Column("extended_authorization") Boolean extendedAuthorization,
        @Column("voucher_amount_cents") Long voucherAmountCents,
        @Column("reward_batch_last_month_elaborated") String rewardBatchLastMonthElaborated,
        @Column("checks_error") Json checksError,
        @Column("accrued_reward_cents") long accruedRewardCents,
        @Column("transaction_revision") long transactionRevision
) {
}
