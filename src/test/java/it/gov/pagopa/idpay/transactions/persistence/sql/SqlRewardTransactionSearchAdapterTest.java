package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardTransactionSearchAdapterTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String MERCHANT_ID = "merchant-1";
    private static final String POINT_OF_SALE_ID = "pos-1";
    private static final String USER_ID = "user-1";
    private static final String BATCH_ID = "batch-1";

    private static SqlRewardTransactionAdapter transactionWriter;
    private static SqlRewardTransactionSearchAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        var dslContext = DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                SQLDialect.POSTGRES
        );
        RewardTransactionSqlMapper mapper = new RewardTransactionSqlMapper(JsonMapper.builder().build());
        transactionWriter = new SqlRewardTransactionAdapter(
                transactionalOperator(),
                dslContext,
                mapper
        );
        adapter = new SqlRewardTransactionSearchAdapter(dslContext, mapper);
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient()
                .sql("DELETE FROM reward_transactions")
                .fetch()
                .rowsUpdated()
                .then(databaseClient()
                        .sql("DELETE FROM reward_batches")
                        .fetch()
                        .rowsUpdated())
                .block();
    }

    @Test
    void shouldSearchAndCountMerchantTransactionsWithConsultableVisibility() {
        RewardTransaction consultable = transaction(
                "merchant-consultable",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                2
        );
        consultable.setRewardBatchId(BATCH_ID);
        consultable.setTrxCode("merchant-code-a");

        RewardTransaction toCheck = transaction(
                "merchant-to-check",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.TO_CHECK,
                1
        );
        toCheck.setRewardBatchId(BATCH_ID);
        toCheck.setTrxCode("merchant-code-b");

        RewardTransaction differentUser = transaction(
                "merchant-other-user",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                3
        );
        differentUser.setRewardBatchId(BATCH_ID);
        differentUser.setTrxCode("merchant-code-c");
        differentUser.setUserId("other-user");

        RewardTransaction approved = transaction(
                "merchant-approved",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.APPROVED,
                4
        );
        approved.setRewardBatchId(BATCH_ID);
        approved.setTrxCode("merchant-code-d");

        TrxFiltersDTO filters = TrxFiltersDTO.builder()
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .rewardBatchId(BATCH_ID)
                .trxCode("MERCHANT-CODE")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .build();

        StepVerifier.create(createBatch()
                        .then(seed(consultable, toCheck, differentUser, approved))
                        .thenMany(adapter.findMerchantTransactions(
                                filters,
                                USER_ID,
                                true,
                                PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "trxCode"))
                        )))
                .assertNext(transaction -> assertEquals("merchant-consultable", transaction.getId()))
                .assertNext(transaction -> assertEquals("merchant-to-check", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.countMerchantTransactions(filters, USER_ID, true))
                .expectNext(2L)
                .verifyComplete();

        StepVerifier.create(adapter.findMerchantTransactions(
                        filters,
                        USER_ID,
                        true,
                        PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "id"))
                ))
                .expectNextCount(2)
                .verifyComplete();

        StepVerifier.create(adapter.findMerchantTransactions(
                        filters,
                        USER_ID,
                        true,
                        PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "transactionId"))
                ))
                .expectNextCount(2)
                .verifyComplete();

        StepVerifier.create(adapter.findMerchantTransactions(
                        filters,
                        USER_ID,
                        false,
                        PageRequest.of(0, 10)
                ))
                .expectNextCount(1)
                .verifyComplete();

        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
        StepVerifier.create(adapter.findMerchantTransactions(
                        filters,
                        USER_ID,
                        true,
                        PageRequest.of(0, 10)
                ))
                .assertNext(transaction -> assertEquals("merchant-approved", transaction.getId()))
                .verifyComplete();
    }

    @Test
    void shouldPagePointOfSaleTransactionsByBusinessStatusAndProductGtin() {
        RewardTransaction cancelled = transaction(
                "point-of-sale-cancelled",
                SyncTrxStatus.CANCELLED,
                RewardBatchTrxStatus.CONSULTABLE,
                1
        );
        RewardTransaction invoiced = transaction(
                "point-of-sale-invoiced",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                2
        );
        RewardTransaction rewarded = transaction(
                "point-of-sale-rewarded",
                SyncTrxStatus.REWARDED,
                RewardBatchTrxStatus.CONSULTABLE,
                3
        );
        RewardTransaction refunded = transaction(
                "point-of-sale-refunded",
                SyncTrxStatus.REFUNDED,
                RewardBatchTrxStatus.CONSULTABLE,
                4
        );
        RewardTransaction unsupportedStatus = transaction(
                "point-of-sale-authorized",
                SyncTrxStatus.AUTHORIZED,
                RewardBatchTrxStatus.CONSULTABLE,
                5
        );
        RewardTransaction differentProduct = transaction(
                "point-of-sale-different-product",
                SyncTrxStatus.CANCELLED,
                RewardBatchTrxStatus.CONSULTABLE,
                6
        );
        differentProduct.setAdditionalProperties(Map.of("productName", "Other", "productGtin", "different"));

        TrxFiltersDTO filters = TrxFiltersDTO.builder()
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .build();
        PageRequest firstPage = PageRequest.of(0, 2, Sort.by(Sort.Direction.ASC, "status"));
        PageRequest secondPage = PageRequest.of(1, 2, Sort.by(Sort.Direction.ASC, "status"));

        StepVerifier.create(seed(
                        refunded,
                        rewarded,
                        invoiced,
                        cancelled,
                        unsupportedStatus,
                        differentProduct
                ).thenMany(adapter.findPointOfSaleTransactions(
                        filters,
                        POINT_OF_SALE_ID,
                        USER_ID,
                        "ABC",
                        false,
                        firstPage
                )))
                .assertNext(transaction -> assertEquals("point-of-sale-cancelled", transaction.getId()))
                .assertNext(transaction -> assertEquals("point-of-sale-invoiced", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findPointOfSaleTransactions(
                        filters,
                        POINT_OF_SALE_ID,
                        USER_ID,
                        "ABC",
                        false,
                        secondPage
                ))
                .assertNext(transaction -> assertEquals("point-of-sale-rewarded", transaction.getId()))
                .assertNext(transaction -> assertEquals("point-of-sale-refunded", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findPointOfSaleTransactions(
                        filters,
                        POINT_OF_SALE_ID,
                        USER_ID,
                        "ABC",
                        false,
                        PageRequest.of(0, 2, Sort.by(Sort.Direction.DESC, "status"))
                ))
                .assertNext(transaction -> assertEquals("point-of-sale-refunded", transaction.getId()))
                .assertNext(transaction -> assertEquals("point-of-sale-rewarded", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.countPointOfSaleTransactions(
                        filters,
                        POINT_OF_SALE_ID,
                        "ABC",
                        USER_ID,
                        false
                ))
                .expectNext(4L)
                .verifyComplete();
    }

    @Test
    void shouldUseFallbacksForNullableFiltersAndPageables() {
        RewardTransaction fallback = transaction(
                "fallback",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                1
        );
        TrxFiltersDTO filters = TrxFiltersDTO.builder()
                .merchantId(MERCHANT_ID)
                .initiativeId(INITIATIVE_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .build();

        StepVerifier.create(seed(fallback)
                        .thenMany(adapter.findMerchantTransactions(filters, null, false, null)))
                .assertNext(transaction -> assertEquals("fallback", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findMerchantTransactions(
                        filters,
                        null,
                        false,
                        Pageable.unpaged()
                ))
                .assertNext(transaction -> assertEquals("fallback", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findPointOfSaleTransactions(
                        filters,
                        POINT_OF_SALE_ID,
                        USER_ID,
                        " ",
                        false,
                        null
                ))
                .assertNext(transaction -> assertEquals("fallback", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findPointOfSaleTransactions(
                        filters,
                        POINT_OF_SALE_ID,
                        USER_ID,
                        " ",
                        false,
                        Pageable.unpaged()
                ))
                .assertNext(transaction -> assertEquals("fallback", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findPointOfSaleTransactions(
                        filters,
                        POINT_OF_SALE_ID,
                        USER_ID,
                        " ",
                        false,
                        PageRequest.of(0, 10)
                ))
                .assertNext(transaction -> assertEquals("fallback", transaction.getId()))
                .verifyComplete();
    }

    @Test
    void shouldReadBatchTransactionsInDeterministicSamplingOrder() {
        RewardTransaction first = transaction(
                "batch-sample-a",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                1
        );
        first.setRewardBatchId(BATCH_ID);
        RewardTransaction second = transaction(
                "batch-sample-b",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.TO_CHECK,
                1
        );
        second.setRewardBatchId(BATCH_ID);
        RewardTransaction third = transaction(
                "batch-sample-c",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                2
        );
        third.setRewardBatchId(BATCH_ID);
        RewardTransaction rejected = transaction(
                "batch-rejected",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.REJECTED,
                0
        );
        rejected.setRewardBatchId(BATCH_ID);

        StepVerifier.create(createBatch()
                        .then(seed(third, second, rejected, first))
                        .thenMany(adapter.findBatchTransactions(
                                BATCH_ID,
                                INITIATIVE_ID,
                                List.of(RewardBatchTrxStatus.CONSULTABLE, RewardBatchTrxStatus.TO_CHECK)
                        )))
                .assertNext(transaction -> assertEquals("batch-sample-a", transaction.getId()))
                .assertNext(transaction -> assertEquals("batch-sample-b", transaction.getId()))
                .assertNext(transaction -> assertEquals("batch-sample-c", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findTransactionInBatch(
                        INITIATIVE_ID,
                        MERCHANT_ID,
                        BATCH_ID,
                        "batch-sample-b"
                ))
                .assertNext(transaction -> assertEquals("batch-sample-b", transaction.getId()))
                .verifyComplete();
    }

    @Test
    void shouldSelectOnlyCsvEligibleRowsFromTheRequestedBatchAndRetainCsvSourceData() {
        RewardTransaction approved = transaction(
                "csv-approved",
                SyncTrxStatus.REWARDED,
                RewardBatchTrxStatus.APPROVED,
                2
        );
        approved.setRewardBatchId(BATCH_ID);

        RewardTransaction rejected = transaction(
                "csv-rejected",
                SyncTrxStatus.REWARDED,
                RewardBatchTrxStatus.REJECTED,
                1
        );
        rejected.setRewardBatchId(BATCH_ID);

        RewardTransaction pending = transaction(
                "csv-pending",
                SyncTrxStatus.REWARDED,
                RewardBatchTrxStatus.CONSULTABLE,
                0
        );
        pending.setRewardBatchId(BATCH_ID);

        RewardTransaction otherBatch = transaction(
                "csv-other-batch",
                SyncTrxStatus.REWARDED,
                RewardBatchTrxStatus.APPROVED,
                0
        );
        otherBatch.setRewardBatchId("batch-2");

        StepVerifier.create(createBatch()
                        .then(createBatch("batch-2", "2026-08"))
                        .then(seed(approved, rejected, pending, otherBatch))
                        .thenMany(adapter.findBatchTransactions(
                                BATCH_ID,
                                INITIATIVE_ID,
                                List.of(RewardBatchTrxStatus.APPROVED, RewardBatchTrxStatus.REJECTED)
                        ))
                        .collectList())
                .assertNext(transactions -> {
                    assertEquals(List.of("csv-rejected", "csv-approved"),
                            transactions.stream().map(RewardTransaction::getId).toList());
                    assertEquals("csv-rejected", transactions.getFirst().getTrxCode());
                    assertEquals("Coffee", transactions.getLast().getAdditionalProperties().get("productName"));
                    assertEquals("AbC-123", transactions.getLast().getAdditionalProperties().get("productGtin"));
                })
                .verifyComplete();
    }

    @Test
    void shouldReturnNoBatchTransactionsForNullOrEmptyStatuses() {
        StepVerifier.create(adapter.findBatchTransactions(BATCH_ID, INITIATIVE_ID, null))
                .verifyComplete();

        StepVerifier.create(adapter.findBatchTransactions(
                        BATCH_ID,
                        INITIATIVE_ID,
                        List.<RewardBatchTrxStatus>of()
                ))
                .verifyComplete();
    }

    @Test
    void shouldOnlyFindInvoiceEligibleTransactionsForTheMerchant() {
        RewardTransaction eligible = transaction(
                "invoice-eligible",
                SyncTrxStatus.REWARDED,
                RewardBatchTrxStatus.CONSULTABLE,
                1
        );
        RewardTransaction ineligible = transaction(
                "invoice-ineligible",
                SyncTrxStatus.CANCELLED,
                RewardBatchTrxStatus.CONSULTABLE,
                2
        );

        StepVerifier.create(seed(eligible, ineligible)
                        .then(adapter.findInvoiceTransaction(MERCHANT_ID, "invoice-eligible")))
                .assertNext(transaction -> assertEquals("invoice-eligible", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findInvoiceTransaction(MERCHANT_ID, "invoice-ineligible"))
                .verifyComplete();
    }

    @Test
    void shouldReturnDistinctFranchiseAndPointOfSalePairsInStableOrderWithinMerchantBatch() {
        RewardTransaction first = transaction("pair-first", SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE, 1);
        first.setRewardBatchId(BATCH_ID);
        first.setFranchiseName("Alpha");
        first.setPointOfSaleId("pos-2");
        RewardTransaction duplicate = transaction("pair-duplicate", SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE, 2);
        duplicate.setRewardBatchId(BATCH_ID);
        duplicate.setFranchiseName("Alpha");
        duplicate.setPointOfSaleId("pos-2");
        RewardTransaction second = transaction("pair-second", SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE, 3);
        second.setRewardBatchId(BATCH_ID);
        second.setFranchiseName("Beta");
        second.setPointOfSaleId("pos-1");
        RewardTransaction otherMerchant = transaction("pair-other-merchant", SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE, 4);
        otherMerchant.setRewardBatchId(BATCH_ID);
        otherMerchant.setMerchantId("other-merchant");
        otherMerchant.setFranchiseName("Aardvark");
        otherMerchant.setPointOfSaleId("pos-0");

        StepVerifier.create(createBatch()
                        .then(seed(second, duplicate, otherMerchant, first))
                        .thenMany(adapter.findDistinctFranchiseAndPosByRewardBatchId(BATCH_ID, MERCHANT_ID)))
                .assertNext(pair -> {
                    assertEquals("Alpha", pair.getFranchiseName());
                    assertEquals("pos-2", pair.getPointOfSaleId());
                })
                .assertNext(pair -> {
                    assertEquals("Beta", pair.getFranchiseName());
                    assertEquals("pos-1", pair.getPointOfSaleId());
                })
                .verifyComplete();
    }

    @Test
    void shouldFilterIssuerHistoryByOptionalUserAmountInclusiveDatesAndDatabasePaging() {
        LocalDateTime start = LocalDateTime.of(2026, Month.JULY, 10, 0, 0);
        LocalDateTime end = LocalDateTime.of(2026, Month.JULY, 12, 0, 0);
        RewardTransaction atStart = historyTransaction("issuer-start", "issuer", USER_ID, start, 100L);
        RewardTransaction middle = historyTransaction("issuer-middle", "issuer", USER_ID, start.plusDays(1), 100L);
        RewardTransaction atEnd = historyTransaction("issuer-end", "issuer", USER_ID, end, 100L);
        RewardTransaction differentAmount = historyTransaction("issuer-other-amount", "issuer", USER_ID,
                start.plusDays(1), 200L);
        RewardTransaction differentUser = historyTransaction("issuer-other-user", "issuer", "other-user",
                start.plusDays(1), 100L);
        RewardTransaction outside = historyTransaction("issuer-outside", "issuer", USER_ID,
                end.plusSeconds(1), 100L);

        StepVerifier.create(seed(atEnd, differentAmount, outside, middle, differentUser, atStart)
                        .thenMany(adapter.findByIdTrxIssuer(
                                "issuer", USER_ID, start, end, 100L,
                                PageRequest.of(0, 2, Sort.by(Sort.Direction.ASC, "trxDate")))))
                .assertNext(transaction -> assertEquals("issuer-start", transaction.getId()))
                .assertNext(transaction -> assertEquals("issuer-middle", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findByIdTrxIssuer(
                        "issuer", null, start, end, null,
                        PageRequest.of(1, 2, Sort.by(Sort.Direction.ASC, "trxDate"))))
                .assertNext(transaction -> assertEquals("issuer-other-amount", transaction.getId()))
                .assertNext(transaction -> assertEquals("issuer-other-user", transaction.getId()))
                .verifyComplete();
    }

    @Test
    void shouldSearchRangeAndInitiativeUserOnlyWithinRequestedScope() {
        LocalDateTime date = LocalDateTime.of(2026, Month.JULY, 10, 10, 0);
        RewardTransaction matching = historyTransaction("range-match", "issuer", USER_ID, date, 100L);
        RewardTransaction second = historyTransaction("range-second", "issuer", USER_ID, date.plusHours(1), 100L);
        RewardTransaction otherInitiative = historyTransaction("range-other-initiative", "issuer", USER_ID, date, 100L);
        RewardTransaction otherUser = historyTransaction("range-other-user", "issuer", "other-user", date, 100L);

        StepVerifier.create(seed(second, otherInitiative, otherUser, matching)
                        .then(databaseClient()
                                .sql("UPDATE reward_transactions SET initiative_id = 'other-initiative' "
                                        + "WHERE transaction_id = 'range-other-initiative'")
                                .fetch()
                                .rowsUpdated())
                        .thenMany(adapter.findByRange(
                                USER_ID, date, date.plusHours(1), 100L,
                                PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "trxDate")))))
                .assertNext(transaction -> assertEquals("range-second", transaction.getId()))
                .assertNext(transaction -> assertEquals("range-match", transaction.getId()))
                .assertNext(transaction -> assertEquals("range-other-initiative", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findByInitiativeIdAndUserId(INITIATIVE_ID, USER_ID))
                .assertNext(transaction -> assertEquals("range-match", transaction.getId()))
                .assertNext(transaction -> assertEquals("range-second", transaction.getId()))
                .verifyComplete();
    }

    @Test
    void shouldUseDeterministicHistoryFallbackOrderingForNullAndUnpagedPageables() {
        RewardTransaction first = historyTransaction(
                "history-a", "issuer", USER_ID, LocalDateTime.of(2026, Month.JULY, 10, 10, 0), 100L);
        RewardTransaction second = historyTransaction(
                "history-b", "issuer", USER_ID, LocalDateTime.of(2026, Month.JULY, 11, 10, 0), 200L);

        StepVerifier.create(seed(second, first)
                        .thenMany(adapter.findByIdTrxIssuer(
                                "issuer", null, null, null, null, null)))
                .assertNext(transaction -> assertEquals("history-a", transaction.getId()))
                .assertNext(transaction -> assertEquals("history-b", transaction.getId()))
                .verifyComplete();

        StepVerifier.create(adapter.findByRange(
                        USER_ID, null, null, null, Pageable.unpaged()))
                .assertNext(transaction -> assertEquals("history-a", transaction.getId()))
                .assertNext(transaction -> assertEquals("history-b", transaction.getId()))
                .verifyComplete();
    }

    private Mono<Void> createBatch() {
        return createBatch(BATCH_ID);
    }

    private Mono<Void> createBatch(String batchId) {
        return createBatch(batchId, "2026-07");
    }

    private Mono<Void> createBatch(String batchId, String month) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level
                        )
                        VALUES (:id, :initiativeId, :merchantId, :month, 'PHYSICAL', 'CREATED', 'July', 'L1')
                        """)
                .bind("id", batchId)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("merchantId", MERCHANT_ID)
                .bind("month", month)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> seed(RewardTransaction... transactions) {
        return Flux.fromArray(transactions)
                .concatMap(transactionWriter::upsert)
                .then();
    }

    private static RewardTransaction transaction(
            String id,
            SyncTrxStatus status,
            RewardBatchTrxStatus rewardBatchTrxStatus,
            int samplingKey
    ) {
        return RewardTransaction.builder()
                .id(id)
                .initiatives(List.of(INITIATIVE_ID))
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .userId(USER_ID)
                .status(status.name())
                .trxCode(id)
                .trxChargeDate(LocalDateTime.of(2026, Month.JULY, 1, 10, samplingKey))
                .rewardBatchTrxStatus(rewardBatchTrxStatus)
                .samplingKey(samplingKey)
                .additionalProperties(Map.of("productName", "Coffee", "productGtin", "AbC-123"))
                .rewards(Map.of(INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build()))
                .build();
    }

    private static RewardTransaction historyTransaction(
            String id,
            String issuer,
            String user,
            LocalDateTime trxDate,
            long amountCents
    ) {
        RewardTransaction transaction = transaction(
                id, SyncTrxStatus.INVOICED, RewardBatchTrxStatus.CONSULTABLE, 0);
        transaction.setIdTrxIssuer(issuer);
        transaction.setUserId(user);
        transaction.setTrxDate(trxDate);
        transaction.setAmountCents(amountCents);
        return transaction;
    }

    @Test
    void shouldFindBatchTransactionIds() {
        RewardTransaction trx1 = transaction(
                "batch-trx-1",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                1
        );
        trx1.setRewardBatchId(BATCH_ID);

        RewardTransaction trx2 = transaction(
                "batch-trx-2",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.TO_CHECK,
                2
        );
        trx2.setRewardBatchId(BATCH_ID);

        RewardTransaction otherBatchTrx = transaction(
                "other-batch-trx",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                3
        );
        otherBatchTrx.setRewardBatchId("other-batch");

        RewardTransaction otherInitiativeTrx = transaction(
                "other-initiative-trx",
                SyncTrxStatus.INVOICED,
                RewardBatchTrxStatus.CONSULTABLE,
                4
        );
        otherInitiativeTrx.setRewardBatchId("other-initiative-batch");
        otherInitiativeTrx.setInitiatives(List.of("other-initiative"));

        StepVerifier.create(createBatch()
                        .then(createBatch("other-batch", "2026-08"))
                        .then(createCustomBatch("other-initiative-batch", "other-initiative", MERCHANT_ID, "2026-08"))
                        .then(seed(trx1, trx2, otherBatchTrx, otherInitiativeTrx))
                        .thenMany(adapter.findBatchTransactionIds(BATCH_ID, INITIATIVE_ID)))
                .expectNextMatches(id -> List.of("batch-trx-1", "batch-trx-2").contains(id))
                .expectNextMatches(id -> List.of("batch-trx-1", "batch-trx-2").contains(id))
                .verifyComplete();
    }

    private Mono<Void> createCustomBatch(String batchId, String initiativeId, String merchantId, String month) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level
                        )
                        VALUES (:id, :initiativeId, :merchantId, :month, 'PHYSICAL', 'CREATED', 'July', 'L1')
                        """)
                .bind("id", batchId)
                .bind("initiativeId", initiativeId)
                .bind("merchantId", merchantId)
                .bind("month", month)
                .fetch()
                .rowsUpdated()
                .then();
    }
}
