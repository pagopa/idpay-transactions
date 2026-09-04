package it.gov.pagopa.idpay.transactions.persistence.sql;

import static it.gov.pagopa.idpay.transactions.persistence.sql.generated.tables.RewardTransactions.REWARD_TRANSACTIONS;
import static org.jooq.impl.DSL.case_;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.falseCondition;
import static org.jooq.impl.DSL.jsonbGetAttributeAsText;

import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.persistence.port.InvoiceTransactionLookupPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchTransactionReadPort;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardTransactionSearchPort;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SortField;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Component
public class SqlRewardTransactionSearchAdapter implements
        RewardTransactionSearchPort,
        RewardBatchTransactionReadPort,
        InvoiceTransactionLookupPort {

    private static final Field<String> PRODUCT_NAME = jsonbGetAttributeAsText(
            REWARD_TRANSACTIONS.ADDITIONAL_PROPERTIES,
            "productName"
    );
    private static final Field<String> PRODUCT_GTIN = jsonbGetAttributeAsText(
            REWARD_TRANSACTIONS.ADDITIONAL_PROPERTIES,
            "productGtin"
    );
    private static final Field<Integer> STATUS_RANK = case_(REWARD_TRANSACTIONS.STATUS)
            .when(SyncTrxStatus.CANCELLED.name(), 1)
            .when(SyncTrxStatus.INVOICED.name(), 2)
            .when(SyncTrxStatus.REWARDED.name(), 3)
            .when(SyncTrxStatus.REFUNDED.name(), 4)
            .otherwise(99);
    private static final Map<String, Field<?>> SORTABLE_FIELDS = Map.ofEntries(
            Map.entry("id", REWARD_TRANSACTIONS.TRANSACTION_ID),
            Map.entry("transactionId", REWARD_TRANSACTIONS.TRANSACTION_ID),
            Map.entry("initiativeId", REWARD_TRANSACTIONS.INITIATIVE_ID),
            Map.entry("rewardBatchId", REWARD_TRANSACTIONS.REWARD_BATCH_ID),
            Map.entry("idTrxAcquirer", REWARD_TRANSACTIONS.ID_TRX_ACQUIRER),
            Map.entry("acquirerCode", REWARD_TRANSACTIONS.ACQUIRER_CODE),
            Map.entry("trxDate", REWARD_TRANSACTIONS.TRX_DATE),
            Map.entry("operationType", REWARD_TRANSACTIONS.OPERATION_TYPE),
            Map.entry("circuitType", REWARD_TRANSACTIONS.CIRCUIT_TYPE),
            Map.entry("idTrxIssuer", REWARD_TRANSACTIONS.ID_TRX_ISSUER),
            Map.entry("correlationId", REWARD_TRANSACTIONS.CORRELATION_ID),
            Map.entry("amountCents", REWARD_TRANSACTIONS.AMOUNT_CENTS),
            Map.entry("amountCurrency", REWARD_TRANSACTIONS.AMOUNT_CURRENCY),
            Map.entry("acquirerId", REWARD_TRANSACTIONS.ACQUIRER_ID),
            Map.entry("merchantId", REWARD_TRANSACTIONS.MERCHANT_ID),
            Map.entry("pointOfSaleId", REWARD_TRANSACTIONS.POINT_OF_SALE_ID),
            Map.entry("posType", REWARD_TRANSACTIONS.POS_TYPE),
            Map.entry("status", REWARD_TRANSACTIONS.STATUS),
            Map.entry("userId", REWARD_TRANSACTIONS.USER_ID),
            Map.entry("operationTypeTranscoded", REWARD_TRANSACTIONS.OPERATION_TYPE_TRANSCODED),
            Map.entry("effectiveAmountCents", REWARD_TRANSACTIONS.EFFECTIVE_AMOUNT_CENTS),
            Map.entry("trxChargeDate", REWARD_TRANSACTIONS.TRX_CHARGE_DATE),
            Map.entry("elaborationDateTime", REWARD_TRANSACTIONS.ELABORATION_DATE_TIME),
            Map.entry("channel", REWARD_TRANSACTIONS.CHANNEL),
            Map.entry("trxCode", REWARD_TRANSACTIONS.TRX_CODE),
            Map.entry("rewardBatchTrxStatus", REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS),
            Map.entry("rewardBatchInclusionDate", REWARD_TRANSACTIONS.REWARD_BATCH_INCLUSION_DATE),
            Map.entry("franchiseName", REWARD_TRANSACTIONS.FRANCHISE_NAME),
            Map.entry("pointOfSaleType", REWARD_TRANSACTIONS.POINT_OF_SALE_TYPE),
            Map.entry("businessName", REWARD_TRANSACTIONS.BUSINESS_NAME),
            Map.entry("invoiceUploadDate", REWARD_TRANSACTIONS.INVOICE_UPLOAD_DATE),
            Map.entry("samplingKey", REWARD_TRANSACTIONS.SAMPLING_KEY),
            Map.entry("updateDate", REWARD_TRANSACTIONS.UPDATE_DATE),
            Map.entry("extendedAuthorization", REWARD_TRANSACTIONS.EXTENDED_AUTHORIZATION),
            Map.entry("voucherAmountCents", REWARD_TRANSACTIONS.VOUCHER_AMOUNT_CENTS),
            Map.entry("rewardBatchLastMonthElaborated", REWARD_TRANSACTIONS.REWARD_BATCH_LAST_MONTH_ELABORATED),
            Map.entry("accruedRewardCents", REWARD_TRANSACTIONS.ACCRUED_REWARD_CENTS),
            Map.entry("productName", PRODUCT_NAME),
            Map.entry("additionalProperties.productName", PRODUCT_NAME),
            Map.entry("productGtin", PRODUCT_GTIN),
            Map.entry("additionalProperties.productGtin", PRODUCT_GTIN)
    );

    private final DSLContext dslContext;
    private final RewardTransactionSqlMapper mapper;

    @Override
    public Flux<RewardTransaction> findMerchantTransactions(
            TrxFiltersDTO filters,
            String userId,
            boolean includeToCheckWithConsultable,
            Pageable pageable
    ) {
        return selectTransactions(
                searchCondition(
                        filters,
                        filters.getPointOfSaleId(),
                        userId,
                        null,
                        includeToCheckWithConsultable
                ),
                transactionSortFields(pageable),
                pageable
        );
    }

    @Override
    public Mono<Long> countMerchantTransactions(
            TrxFiltersDTO filters,
            String userId,
            boolean includeToCheckWithConsultable
    ) {
        return countTransactions(searchCondition(
                filters,
                filters.getPointOfSaleId(),
                userId,
                null,
                includeToCheckWithConsultable
        ));
    }

    @Override
    public Flux<RewardTransaction> findPointOfSaleTransactions(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String userId,
            String productGtin,
            boolean includeToCheckWithConsultable,
            Pageable pageable
    ) {
        Pageable effectivePageable = pointOfSalePageable(pageable);
        return selectTransactions(
                searchCondition(filters, pointOfSaleId, userId, productGtin, includeToCheckWithConsultable),
                pointOfSaleSortFields(effectivePageable.getSort()),
                effectivePageable
        );
    }

    @Override
    public Mono<Long> countPointOfSaleTransactions(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String productGtin,
            String userId,
            boolean includeToCheckWithConsultable
    ) {
        return countTransactions(searchCondition(
                filters,
                pointOfSaleId,
                userId,
                productGtin,
                includeToCheckWithConsultable
        ));
    }

    @Override
    public Flux<RewardTransaction> findBatchTransactions(
            String rewardBatchId,
            String initiativeId,
            List<RewardBatchTrxStatus> statuses
    ) {
        Condition statusCondition = statuses == null || statuses.isEmpty()
                ? falseCondition()
                : REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(statuses.stream()
                        .map(Enum::name)
                        .toList());
        return Flux.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                .and(statusCondition))
                        .orderBy(
                                REWARD_TRANSACTIONS.SAMPLING_KEY.asc(),
                                REWARD_TRANSACTIONS.TRANSACTION_ID.asc()
                        ))
                .map(mapper::fromRecord);
    }

    @Override
    public Mono<RewardTransaction> findTransactionInBatch(
            String initiativeId,
            String merchantId,
            String rewardBatchId,
            String transactionId
    ) {
        return Mono.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId))
                                .and(REWARD_TRANSACTIONS.MERCHANT_ID.eq(merchantId))
                                .and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId))))
                .map(mapper::fromRecord);
    }

    @Override
    public Mono<RewardTransaction> findInvoiceTransaction(String merchantId, String transactionId) {
        return Mono.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.MERCHANT_ID.eq(merchantId)
                                .and(REWARD_TRANSACTIONS.TRANSACTION_ID.eq(transactionId))
                                .and(REWARD_TRANSACTIONS.STATUS.in(
                                        SyncTrxStatus.REWARDED.name(),
                                        SyncTrxStatus.REFUNDED.name(),
                                        SyncTrxStatus.INVOICED.name()
                                ))))
                .map(mapper::fromRecord);
    }

    @Override
    public Flux<FranchisePointOfSaleDTO> findDistinctFranchiseAndPosByRewardBatchId(
            String rewardBatchId,
            String merchantId
    ) {
        return Flux.from(dslContext.selectDistinct(
                        REWARD_TRANSACTIONS.FRANCHISE_NAME,
                        REWARD_TRANSACTIONS.POINT_OF_SALE_ID
                )
                .from(REWARD_TRANSACTIONS)
                .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                        .and(REWARD_TRANSACTIONS.MERCHANT_ID.eq(merchantId)))
                .orderBy(
                        REWARD_TRANSACTIONS.FRANCHISE_NAME.asc(),
                        REWARD_TRANSACTIONS.POINT_OF_SALE_ID.asc()
                ))
                .map(result -> FranchisePointOfSaleDTO.builder()
                        .franchiseName(result.get(REWARD_TRANSACTIONS.FRANCHISE_NAME))
                        .pointOfSaleId(result.get(REWARD_TRANSACTIONS.POINT_OF_SALE_ID))
                        .build());
    }

    @Override
    public Flux<RewardTransaction> findByIdTrxIssuer(
            String idTrxIssuer,
            String userId,
            LocalDateTime trxDateStart,
            LocalDateTime trxDateEnd,
            Long amountCents,
            Pageable pageable
    ) {
        Condition condition = REWARD_TRANSACTIONS.ID_TRX_ISSUER.eq(idTrxIssuer);
        if (userId != null) {
            condition = condition.and(REWARD_TRANSACTIONS.USER_ID.eq(userId));
        }
        if (amountCents != null) {
            condition = condition.and(REWARD_TRANSACTIONS.AMOUNT_CENTS.eq(amountCents));
        }
        condition = dateCondition(condition, trxDateStart, trxDateEnd);
        return selectTransactions(condition, transactionSortFields(pageable), pageable);
    }

    @Override
    public Flux<RewardTransaction> findByRange(
            String userId,
            LocalDateTime trxDateStart,
            LocalDateTime trxDateEnd,
            Long amountCents,
            Pageable pageable
    ) {
        Condition condition = REWARD_TRANSACTIONS.USER_ID.eq(userId);
        if (amountCents != null) {
            condition = condition.and(REWARD_TRANSACTIONS.AMOUNT_CENTS.eq(amountCents));
        }
        condition = dateCondition(condition, trxDateStart, trxDateEnd);
        return selectTransactions(condition, transactionSortFields(pageable), pageable);
    }

    @Override
    public Flux<RewardTransaction> findByInitiativeIdAndUserId(String initiativeId, String userId) {
        return Flux.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId)
                                .and(REWARD_TRANSACTIONS.USER_ID.eq(userId)))
                        .orderBy(REWARD_TRANSACTIONS.TRANSACTION_ID.asc()))
                .map(mapper::fromRecord);
    }

    private Flux<RewardTransaction> selectTransactions(
            Condition condition,
            List<? extends SortField<?>> sortFields,
            Pageable pageable
    ) {
        if (pageable == null || pageable.isUnpaged()) {
            return Flux.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                            .where(condition)
                            .orderBy(sortFields))
                    .map(mapper::fromRecord);
        }
        return Flux.from(dslContext.selectFrom(REWARD_TRANSACTIONS)
                        .where(condition)
                        .orderBy(sortFields)
                        .limit(pageable.getPageSize())
                        .offset(pageable.getOffset()))
                .map(mapper::fromRecord);
    }

    private Mono<Long> countTransactions(Condition condition) {
        return Mono.from(dslContext.select(count())
                        .from(REWARD_TRANSACTIONS)
                        .where(condition))
                .map(result -> result.value1().longValue());
    }

    private static Condition searchCondition(
            TrxFiltersDTO filters,
            String pointOfSaleId,
            String userId,
            String productGtin,
            boolean includeToCheckWithConsultable
    ) {
        Condition condition = REWARD_TRANSACTIONS.MERCHANT_ID.eq(filters.getMerchantId())
                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(filters.getInitiativeId()));

        if (userId != null) {
            condition = condition.and(REWARD_TRANSACTIONS.USER_ID.eq(userId));
        }
        if (pointOfSaleId != null) {
            condition = condition.and(REWARD_TRANSACTIONS.POINT_OF_SALE_ID.eq(pointOfSaleId));
        }
        if (notBlank(productGtin)) {
            condition = condition.and(containsIgnoreCase(PRODUCT_GTIN, productGtin));
        }
        if (notBlank(filters.getStatus())) {
            condition = condition.and(REWARD_TRANSACTIONS.STATUS.eq(filters.getStatus()));
        } else {
            condition = condition.and(REWARD_TRANSACTIONS.STATUS.in(
                    SyncTrxStatus.CANCELLED.name(),
                    SyncTrxStatus.REWARDED.name(),
                    SyncTrxStatus.REFUNDED.name(),
                    SyncTrxStatus.INVOICED.name()
            ));
        }
        if (filters.getRewardBatchId() != null) {
            condition = condition.and(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(filters.getRewardBatchId()));
        }
        if (filters.getTrxCode() != null) {
            condition = condition.and(containsIgnoreCase(REWARD_TRANSACTIONS.TRX_CODE, filters.getTrxCode()));
        }
        if (filters.getRewardBatchTrxStatus() != null) {
            if (includeToCheckWithConsultable
                    && filters.getRewardBatchTrxStatus() == RewardBatchTrxStatus.CONSULTABLE) {
                condition = condition.and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.in(
                        RewardBatchTrxStatus.CONSULTABLE.name(),
                        RewardBatchTrxStatus.TO_CHECK.name()
                ));
            } else {
                condition = condition.and(REWARD_TRANSACTIONS.REWARD_BATCH_TRX_STATUS.eq(
                        filters.getRewardBatchTrxStatus().name()
                ));
            }
        }
        return condition;
    }

    private static Pageable pointOfSalePageable(Pageable pageable) {
        if (pageable == null || pageable.isUnpaged() || pageable.getSort().isUnsorted()) {
            return PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "trxChargeDate"));
        }
        return pageable;
    }

    private static List<? extends SortField<?>> transactionSortFields(Pageable pageable) {
        if (pageable == null || pageable.getSort().isUnsorted()) {
            return List.of(REWARD_TRANSACTIONS.TRANSACTION_ID.asc());
        }
        return sortFields(pageable.getSort());
    }

    private static List<? extends SortField<?>> pointOfSaleSortFields(Sort sort) {
        Sort.Order statusOrder = sort.stream()
                .filter(order -> order.getProperty().equalsIgnoreCase("status"))
                .findFirst()
                .orElse(null);
        if (statusOrder != null) {
            return List.of(
                    statusOrder.isAscending() ? STATUS_RANK.asc() : STATUS_RANK.desc(),
                    REWARD_TRANSACTIONS.TRANSACTION_ID.asc()
            );
        }
        return sortFields(sort);
    }

    private static List<? extends SortField<?>> sortFields(Sort sort) {
        List<SortField<?>> sortFields = sort.stream()
                .<SortField<?>>map(order -> sortableField(order.getProperty(), order.getDirection()))
                .toList();
        boolean hasTransactionId = sort.stream()
                .anyMatch(order -> order.getProperty().equalsIgnoreCase("id")
                        || order.getProperty().equalsIgnoreCase("transactionId"));
        return hasTransactionId
                ? sortFields
                : appendTransactionIdSort(sortFields);
    }

    private static List<? extends SortField<?>> appendTransactionIdSort(List<SortField<?>> sortFields) {
        List<SortField<?>> deterministicSortFields = new ArrayList<>(sortFields);
        deterministicSortFields.add(REWARD_TRANSACTIONS.TRANSACTION_ID.asc());
        return deterministicSortFields;
    }

    private static SortField<?> sortableField(String property, Sort.Direction direction) {
        Field<?> field = SORTABLE_FIELDS.getOrDefault(property, REWARD_TRANSACTIONS.TRANSACTION_ID);
        return direction.isAscending() ? field.asc() : field.desc();
    }

    private static Condition containsIgnoreCase(Field<String> field, String value) {
        String pattern = "%" + value
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_") + "%";
        return field.likeIgnoreCase(pattern, '\\');
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static Condition dateCondition(
            Condition condition,
            LocalDateTime trxDateStart,
            LocalDateTime trxDateEnd
    ) {
        if (trxDateStart != null) {
            condition = condition.and(REWARD_TRANSACTIONS.TRX_DATE.ge(trxDateStart));
        }
        if (trxDateEnd != null) {
            condition = condition.and(REWARD_TRANSACTIONS.TRX_DATE.le(trxDateEnd));
        }
        return condition;
    }

    @Override
    public Flux<String> findBatchTransactionIds(String rewardBatchId, String initiativeId, int limit, int offset) {
        return Flux.from(dslContext.select(REWARD_TRANSACTIONS.TRANSACTION_ID)
                        .from(REWARD_TRANSACTIONS)
                        .where(REWARD_TRANSACTIONS.REWARD_BATCH_ID.eq(rewardBatchId)
                                .and(REWARD_TRANSACTIONS.INITIATIVE_ID.eq(initiativeId)))
                        .orderBy(REWARD_TRANSACTIONS.TRANSACTION_ID.asc())
                        .limit(limit)
                        .offset(offset))
                .map(row -> row.get(REWARD_TRANSACTIONS.TRANSACTION_ID));
    }
}
