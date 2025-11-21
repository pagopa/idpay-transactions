package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DirtiesContext
@MongoTest
@Disabled
class RewardTransactionSpecificRepositoryTest {

    @Autowired
    protected RewardTransactionRepository rewardTransactionRepository;

    @Autowired
    private RewardTransactionSpecificRepositoryImpl rewardTransactionSpecificRepository;

    private RewardTransaction rt;
    private RewardTransaction rt1;
    private RewardTransaction rt2;
    private RewardTransaction rt3;
    private static final String INITIATIVE_ID = "INITIATIVEID1";
    private static final String MERCHANT_ID = "MERCHANTID1";
    private static final String USER_ID = "USERID1";
    private static final String POINT_OF_SALE_ID = "POINTOFSALEID1";
    private static final String PRODUCT_GTIN = "PRODUCTGTIN1";

    @BeforeEach
    void setUp() {
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        Long amountCents = 3000L;
        rt = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id_prova")
                .idTrxIssuer("IDTRXISSUER1")
                .trxDate(date)
                .amountCents(amountCents)
                .build();
        rewardTransactionRepository.save(rt).block();
    }

    @AfterEach
    void clearData() {
        rewardTransactionRepository.deleteById("id_prova").onErrorResume(e -> Mono.empty()).block();
        cleanDataPageable();
    }

    @Test
    void findByIdTrxIssuer() {
        Flux<RewardTransaction> resultTrxIssuer = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), null, null, null, null, null);
        Assertions.assertNotNull(resultTrxIssuer);
        List<RewardTransaction> rewardTransactionsList = resultTrxIssuer.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList.size());
        Assertions.assertEquals(List.of(rt), rewardTransactionsList);
    }

    @Test
    void findByIdTrxIssuerAndOptionalFilters() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultTrxIssuerAndUserId = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), rt.getUserId(), null, null, null, null);
        List<RewardTransaction> list1 = resultTrxIssuerAndUserId.toStream().toList();
        assertEquals(1, list1.size());
        assertEquals(List.of(rt), list1);

        Flux<RewardTransaction> resultTrxIssuerAndStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), null, startDate, null, null, null);
        List<RewardTransaction> list2 = resultTrxIssuerAndStartDate.toStream().toList();
        assertEquals(1, list2.size());
        assertEquals(List.of(rt), list2);

        Flux<RewardTransaction> resultTrxIssuerAndEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), null, null, endDate, null, null);
        List<RewardTransaction> list3 = resultTrxIssuerAndEndDate.toStream().toList();
        assertEquals(1, list3.size());
        assertEquals(List.of(rt), list3);

        Flux<RewardTransaction> resultTrxIssuerAndAmount = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), null, null, null, rt.getAmountCents(), null);
        List<RewardTransaction> list4 = resultTrxIssuerAndAmount.toStream().toList();
        assertEquals(1, list4.size());
        assertEquals(List.of(rt), list4);

        Flux<RewardTransaction> resultTrxIssuerAndRangeDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), null, startDate, endDate, rt.getAmountCents(), null);
        List<RewardTransaction> list5 = resultTrxIssuerAndRangeDate.toStream().toList();
        assertEquals(1, list5.size());
        assertEquals(List.of(rt), list5);

        Flux<RewardTransaction> resultBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), null, rt.getTrxDate().plusDays(10L), null, null, null);
        assertNotNull(resultBeforeStartDateBeforeStartDate);
        assertEquals(0, resultBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultDateAfterEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                rt.getIdTrxIssuer(), null, null, rt.getTrxDate().minusDays(10L), null, null);
        assertNotNull(resultDateAfterEndDate);
        assertEquals(0, resultDateAfterEndDate.count().block());
    }

    @Test
    void findByUserIdAndRangeDateAndAmount() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultUserIDAndRangeDate = rewardTransactionSpecificRepository.findByRange(
                rt.getUserId(), startDate, endDate, null, null);
        List<RewardTransaction> list1 = resultUserIDAndRangeDate.toStream().toList();
        assertEquals(1, list1.size());
        assertEquals(List.of(rt), list1);

        Flux<RewardTransaction> resultUserIDAndRangeDateAndAmount = rewardTransactionSpecificRepository.findByRange(
                rt.getUserId(), startDate, endDate, rt.getAmountCents(), null);
        List<RewardTransaction> list2 = resultUserIDAndRangeDateAndAmount.toStream().toList();
        assertEquals(1, list2.size());
        assertEquals(List.of(rt), list2);

        Flux<RewardTransaction> resultUserIDBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByRange(
                rt.getUserId(), rt.getTrxDate().plusDays(10L), endDate, null, null);
        assertEquals(0, resultUserIDBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultUserIDDateAfterEndDate = rewardTransactionSpecificRepository.findByRange(
                rt.getUserId(), startDate, rt.getTrxDate().minusDays(10L), null, null);
        assertEquals(0, resultUserIDDateAfterEndDate.count().block());
    }

    @Test
    void pageableWithfindByIdTrxIssuer() {
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        setUpPageable(date, "userId");

        Pageable pageable = PageRequest.of(0, 2);
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                "IDTRXISSUER", null, null, null, null, pageable);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        assertEquals(2, rewardTransactionsList.size());
        assertEquals(List.of(rt1, rt2), rewardTransactionsList);

        Pageable pageable2 = PageRequest.of(1, 2);
        Flux<RewardTransaction> result2 = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                "IDTRXISSUER", null, null, null, null, pageable2);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        assertEquals(1, rewardTransactionsList2.size());
        assertEquals(List.of(rt3), rewardTransactionsList2);

        Pageable pageable3 = PageRequest.of(0, 2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                "IDTRXISSUER", null, null, null, null, pageable3);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        assertEquals(2, rewardTransactionsList3.size());
        assertEquals(List.of(rt3, rt2), rewardTransactionsList3);

        Pageable pageable4 = PageRequest.of(1, 2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 = rewardTransactionSpecificRepository.findByIdTrxIssuer(
                "IDTRXISSUER", null, null, null, null, pageable4);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        assertEquals(1, rewardTransactionsList4.size());
        assertEquals(List.of(rt1), rewardTransactionsList4);
    }

    @Test
    void pageableWithfindByRange() {
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        LocalDateTime startDate = date.minusDays(10L);
        LocalDateTime endDate = date.plusDays(10L);
        String userId = "USERID";

        setUpPageable(date, userId);

        Pageable pageable = PageRequest.of(0, 2);
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByRange(
                userId, startDate, endDate, null, pageable);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        assertEquals(2, rewardTransactionsList.size());
        assertEquals(List.of(rt1, rt2), rewardTransactionsList);

        Pageable pageable2 = PageRequest.of(1, 2);
        Flux<RewardTransaction> result2 = rewardTransactionSpecificRepository.findByRange(
                userId, startDate, endDate, null, pageable2);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        assertEquals(1, rewardTransactionsList2.size());
        assertEquals(List.of(rt3), rewardTransactionsList2);

        Pageable pageable3 = PageRequest.of(0, 2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 = rewardTransactionSpecificRepository.findByRange(
                userId, startDate, endDate, null, pageable3);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        assertEquals(2, rewardTransactionsList3.size());
        assertEquals(List.of(rt3, rt2), rewardTransactionsList3);

        Pageable pageable4 = PageRequest.of(1, 2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 = rewardTransactionSpecificRepository.findByRange(
                userId, startDate, endDate, null, pageable4);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        assertEquals(1, rewardTransactionsList4.size());
        assertEquals(List.of(rt1), rewardTransactionsList4);
    }

    void setUpPageable(LocalDateTime date, String userId) {
        Long amountCents = 3000L;
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amountCents(amountCents)
                .build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amountCents(amountCents)
                .build();
        rewardTransactionRepository.save(rt2).block();

        rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("id3")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amountCents(amountCents)
                .build();
        rewardTransactionRepository.save(rt3).block();
    }

    void cleanDataPageable() {
        rewardTransactionRepository.deleteById("id1").onErrorResume(e -> Mono.empty()).block();
        rewardTransactionRepository.deleteById("id2").onErrorResume(e -> Mono.empty()).block();
        rewardTransactionRepository.deleteById("id3").onErrorResume(e -> Mono.empty()).block();
    }

    @Test
    void findByFilter() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());
        Flux<RewardTransaction> transactionInProgressList =
                rewardTransactionSpecificRepository.findByFilter(MERCHANT_ID, INITIATIVE_ID, USER_ID,
                        "CANCELLED", null, null, paging);
        List<RewardTransaction> result = transactionInProgressList.toStream().toList();
        assertEquals(rt1, result.get(0));
    }

    @Test
    void findByFilter_withRewardBatchAndInvStatus() {
        String batchId = "BATCH1";

        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status("REWARDED")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Flux<RewardTransaction> resultFlux = rewardTransactionSpecificRepository.findByFilter(
                MERCHANT_ID, INITIATIVE_ID, USER_ID,
                "REWARDED", batchId, RewardBatchTrxStatus.APPROVED, null);

        List<RewardTransaction> result = resultFlux.toStream().toList();
        assertEquals(1, result.size());
        assertEquals(rt1.getId(), result.getFirst().getId());
    }

    @Test
    void findByFilter_withDefaultStatusesWhenStatusNull() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Flux<RewardTransaction> resultFlux = rewardTransactionSpecificRepository.findByFilter(
                MERCHANT_ID, INITIATIVE_ID, USER_ID,
                null, null, null, null);

        List<RewardTransaction> result = resultFlux.toStream().toList();
        assertEquals(1, result.size());
        assertEquals("CANCELLED", result.getFirst().getStatus());
    }

    @Test
    void findByFilterTrx_withSortedPageable_shouldUseProvidedSorting() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .userId(USER_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable sorted = PageRequest.of(0, 10, Sort.by("elaborationDateTime").descending());

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", "REWARDED", sorted);

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());
    }

    @Test
    void findByFilterTrx_withUnsortedPageable_shouldUseDefaultSorting() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .userId(USER_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable unsorted = PageRequest.of(0, 10);
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", "REWARDED", unsorted);

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());
    }

    @Test
    void findByFilterTrx_withNullPageable_shouldUseDefaultPagingAndSorting() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .userId(USER_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", "REWARDED", null);

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());
    }

    @Test
    void findByFilterTrx_withProductGtinFilter() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .userId(USER_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        Map<String, String> additionalProperties = Map.of("productGtin", PRODUCT_GTIN);
        rt1.setAdditionalProperties(additionalProperties);

        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, PRODUCT_GTIN, "REWARDED", pageable);
        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());
    }

    @Test
    void findByFilterTrxWithStatusSortingShouldUseAggregation() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .userId(USER_ID)
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .idTrxIssuer("IDTRXISSUER")
                .userId(USER_ID)
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.save(rt1).block();
        rewardTransactionRepository.save(rt2).block();

        Pageable ascSort = PageRequest.of(0, 10, Sort.by("status"));

        List<RewardTransaction> ascResult = rewardTransactionSpecificRepository.findByFilterTrx(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", null, ascSort
        ).toStream().toList();

        assertEquals(
                List.of(rt1.getId(), rt2.getId()),
                ascResult.stream().map(RewardTransaction::getId).toList()
        );

        Pageable descSort = PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "status"));
        List<RewardTransaction> descResult = rewardTransactionSpecificRepository.findByFilterTrx(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", null, descSort
        ).toStream().toList();

        assertEquals(
                List.of(rt2.getId(), rt1.getId()),
                descResult.stream().map(RewardTransaction::getId).toList()
        );
    }

    @Test
    void getCount() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Mono<Long> count = rewardTransactionSpecificRepository.getCount(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, null, null);
        assertEquals(1L, count.block());
    }

    @Test
    void findOneByInitiativeId() {
        rt = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .status("REWARDED")
                .initiatives(List.of("INITIATIVEID0"))
                .build();
        rewardTransactionRepository.save(rt).block();
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id2")
                .idTrxIssuer("IDTRXISSUER")
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();
        rt2 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id3")
                .idTrxIssuer("IDTRXISSUER")
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt2).block();

        RewardTransaction trx = rewardTransactionSpecificRepository.findOneByInitiativeId(INITIATIVE_ID).block();
        assertNotNull(trx);
        assertTrue(trx.getInitiatives().contains(INITIATIVE_ID));
    }

    @Test
    void findByInitiativesWithBatch() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByInitiativesWithBatch(INITIATIVE_ID, 100);

        List<RewardTransaction> rewardTransactions = result.toStream().toList();
        assertEquals(1, rewardTransactions.size());
    }

    @Test
    void removeInitiativeOnTransaction() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        rewardTransactionSpecificRepository.removeInitiativeOnTransaction(rt1.getId(), INITIATIVE_ID).block();

        RewardTransaction modifiedTrx = rewardTransactionRepository.findById(rt1.getId()).block();
        assertTrue(modifiedTrx.getInitiatives().isEmpty());
    }

    @Test
    void findByFilterTrx_withProductNameSorting_shouldMapToAdditionalPropertiesProductName() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .userId(USER_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "productName"));

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", "REWARDED", pageable);

        List<RewardTransaction> list = result.toStream().toList();

        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.get(0).getId());
    }

    @Test
    void findTransaction_shouldReturnMatchingTransaction_whenStatusIsRewardedOrRefundedOrInvoiced() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .status("REWARDED")
                .build();
        rewardTransactionRepository.save(rt1).block();

        Mono<RewardTransaction> resultMono = rewardTransactionSpecificRepository.findTransaction(
                MERCHANT_ID, POINT_OF_SALE_ID, rt1.getId()
        );

        RewardTransaction result = resultMono.block();

        assertNotNull(result);
        assertEquals(rt1.getId(), result.getId());
        assertEquals(MERCHANT_ID, result.getMerchantId());
        assertEquals(POINT_OF_SALE_ID, result.getPointOfSaleId());
        assertEquals("REWARDED", result.getStatus());
    }

    @Test
    void findByTrxIdAndUserId_shouldReturnMatchingTransaction() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .userId(USER_ID)
                .build();
        rewardTransactionRepository.save(rt1).block();

        RewardTransaction result = rewardTransactionSpecificRepository.findByTrxIdAndUserId("id1", USER_ID).block();

        assertNotNull(result);
        assertEquals("id1", result.getId());
        assertEquals(USER_ID, result.getUserId());
    }
}
