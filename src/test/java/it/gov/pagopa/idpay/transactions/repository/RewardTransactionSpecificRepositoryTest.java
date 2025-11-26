package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.OrganizationRole;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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
    void setUp(){
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
    void clearData(){
        rewardTransactionRepository.deleteById("id_prova").block();
    }

    @Test
    void findByIdTrxIssuer() {
        Flux<RewardTransaction> resultTrxIssuer =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, null, null, null, null);

        Assertions.assertNotNull(resultTrxIssuer);
        List<RewardTransaction> rewardTransactionsList = resultTrxIssuer.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList.size());
        Assertions.assertEquals(List.of(rt), rewardTransactionsList);
    }

    @Test
    void findByIdTrxIssuerAndOptionalFilters() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultTrxIssuerAndUserId =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), rt.getUserId(), null, null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndUserId);
        List<RewardTransaction> resultTrxIssuerAndUserIdList = resultTrxIssuerAndUserId.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndUserIdList.size());
        Assertions.assertEquals(List.of(rt), resultTrxIssuerAndUserIdList);

        Flux<RewardTransaction> resultTrxIssuerAndStartDate =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, startDate, null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndStartDate);
        List<RewardTransaction> resultTrxIssuerAndStartDateList = resultTrxIssuerAndStartDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndStartDateList.size());
        Assertions.assertEquals(List.of(rt), resultTrxIssuerAndStartDateList);

        Flux<RewardTransaction> resultTrxIssuerAndEndDate =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, null, endDate, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndEndDate);
        List<RewardTransaction> resultTrxIssuerAndEndDateList = resultTrxIssuerAndEndDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndEndDateList.size());
        Assertions.assertEquals(List.of(rt), resultTrxIssuerAndEndDateList);

        Flux<RewardTransaction> resultTrxIssuerAndAmount =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, null, null, rt.getAmountCents(), null);
        Assertions.assertNotNull(resultTrxIssuerAndAmount);
        List<RewardTransaction> resultTrxIssuerAndAmountList = resultTrxIssuerAndAmount.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndAmountList.size());
        Assertions.assertEquals(List.of(rt), resultTrxIssuerAndAmountList);

        Flux<RewardTransaction> resultTrxIssuerAndRangeDate =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, startDate, endDate, rt.getAmountCents(), null);
        Assertions.assertNotNull(resultTrxIssuerAndRangeDate);
        List<RewardTransaction> resultTrxIssuerAndRangeDateList = resultTrxIssuerAndRangeDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndRangeDateList.size());
        Assertions.assertEquals(List.of(rt), resultTrxIssuerAndRangeDateList);

        Flux<RewardTransaction> resultBeforeStartDate =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, rt.getTrxDate().plusDays(10L), null, null, null);
        Assertions.assertNotNull(resultBeforeStartDate);
        Assertions.assertEquals(0, resultBeforeStartDate.count().block());

        Flux<RewardTransaction> resultDateAfterEndDate =
                rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, null, rt.getTrxDate().minusDays(10L), null, null);
        Assertions.assertNotNull(resultDateAfterEndDate);
        Assertions.assertEquals(0, resultDateAfterEndDate.count().block());
    }

    @Test
    void findByUserIdAndRangeDateAndAmount() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultUserIDAndRangeDate =
                rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate, endDate, null, null);
        Assertions.assertNotNull(resultUserIDAndRangeDate);
        List<RewardTransaction> resultUserIDAndRangeDateList = resultUserIDAndRangeDate.toStream().toList();
        Assertions.assertEquals(1, resultUserIDAndRangeDateList.size());
        Assertions.assertEquals(List.of(rt), resultUserIDAndRangeDateList);

        Flux<RewardTransaction> resultUserIDAndRangeDateAndAmount =
                rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate, endDate, rt.getAmountCents(), null);
        Assertions.assertNotNull(resultUserIDAndRangeDateAndAmount);
        List<RewardTransaction> resultUserIDAndRangeDateAndAmountList = resultUserIDAndRangeDateAndAmount.toStream().toList();
        Assertions.assertEquals(1, resultUserIDAndRangeDateAndAmountList.size());
        Assertions.assertEquals(List.of(rt), resultUserIDAndRangeDateAndAmountList);

        Flux<RewardTransaction> resultUserIDAfterStartDate =
                rewardTransactionSpecificRepository.findByRange(rt.getUserId(), rt.getTrxDate().plusDays(10L), endDate, null, null);
        Assertions.assertNotNull(resultUserIDAfterStartDate);
        Assertions.assertEquals(0, resultUserIDAfterStartDate.count().block());

        Flux<RewardTransaction> resultUserIDBeforeEndDate =
                rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate, rt.getTrxDate().minusDays(10L), null, null);
        Assertions.assertNotNull(resultUserIDBeforeEndDate);
        Assertions.assertEquals(0, resultUserIDBeforeEndDate.count().block());
    }

    @Test
    void pageableWithfindByIdTrxIssuer(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        setUpPageable(date, "userId");

        Pageable pageable = PageRequest.of(0,2);
        Flux<RewardTransaction> result =
                rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList.size());
        Assertions.assertEquals(List.of(rt1, rt2), rewardTransactionsList);

        Pageable pageable2 = PageRequest.of(1,2);
        Flux<RewardTransaction> result2 =
                rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable2);
        Assertions.assertNotNull(result2);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList2.size());
        Assertions.assertEquals(List.of(rt3), rewardTransactionsList2);

        Pageable pageable3 = PageRequest.of(0,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 =
                rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable3);
        Assertions.assertNotNull(result3);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList3.size());
        Assertions.assertEquals(List.of(rt3, rt2), rewardTransactionsList3);

        Pageable pageable4 = PageRequest.of(1,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 =
                rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable4);
        Assertions.assertNotNull(result4);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList4.size());
        Assertions.assertEquals(List.of(rt1), rewardTransactionsList4);

        cleanDataPageable();
    }

    @Test
    void pageableWithfindByRange(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        LocalDateTime startDate = date.minusDays(10L);
        LocalDateTime endDate = date.plusDays(10L);
        String userId = "USERID";

        setUpPageable(date, userId);

        Pageable pageable = PageRequest.of(0,2);
        Flux<RewardTransaction> result =
                rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList.size());
        Assertions.assertEquals(List.of(rt1, rt2), rewardTransactionsList);

        Pageable pageable2 = PageRequest.of(1,2);
        Flux<RewardTransaction> result2 =
                rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable2);
        Assertions.assertNotNull(result2);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList2.size());
        Assertions.assertEquals(List.of(rt3), rewardTransactionsList2);

        Pageable pageable3 = PageRequest.of(0,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 =
                rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable3);
        Assertions.assertNotNull(result3);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList3.size());
        Assertions.assertEquals(List.of(rt3, rt2), rewardTransactionsList3);

        Pageable pageable4 = PageRequest.of(1,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 =
                rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable4);
        Assertions.assertNotNull(result4);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList4.size());
        Assertions.assertEquals(List.of(rt1), rewardTransactionsList4);

        cleanDataPageable();
    }

    void setUpPageable(LocalDateTime date, String userId){
        Long amountCents = 3000L;
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amountCents(amountCents).build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amountCents(amountCents).build();
        rewardTransactionRepository.save(rt2).block();

        rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("id3")
                .idTrxIssuer("IDTRXISSUER")
                .userId(userId)
                .trxDate(date)
                .amountCents(amountCents).build();
        rewardTransactionRepository.save(rt3).block();
    }

    void cleanDataPageable(){
        rewardTransactionRepository.deleteById("id1").block();
        rewardTransactionRepository.deleteById("id2").block();
        rewardTransactionRepository.deleteById("id3").block();
    }

    @Test
    void findByFilter() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID))
                .userId(USER_ID)
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                USER_ID,
                "CANCELLED",
                null,
                null,
                null
        );

        Flux<RewardTransaction> transactionInProgressList =
                rewardTransactionSpecificRepository.findByFilter(filters, USER_ID, OrganizationRole.MERCHANT, paging);

        List<RewardTransaction> result = transactionInProgressList.toStream().toList();
        assertEquals(1, result.size());
        assertEquals(rt1, result.get(0));

        cleanDataPageable();
    }

    @Test
    void findByFilterTrx_withSortedPageable_shouldUseProvidedSorting() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .userId(USER_ID)
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable sorted = PageRequest.of(0, 10, Sort.by("elaborationDateTime").descending());

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                "REWARDED",
                null,
                null,
                null
        );

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                filters,
                POINT_OF_SALE_ID,
                USER_ID,
                "",
                OrganizationRole.MERCHANT,
                sorted
        );

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());

        cleanDataPageable();
    }

    @Test
    void findByFilterTrx_withUnsortedPageable_shouldUseDefaultSorting() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .userId(USER_ID)
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable unsorted = PageRequest.of(0, 10);

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                "REWARDED",
                null,
                null,
                null
        );

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                filters,
                POINT_OF_SALE_ID,
                USER_ID,
                "",
                OrganizationRole.MERCHANT,
                unsorted
        );

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());

        cleanDataPageable();
    }

    @Test
    void findByFilterTrx_withProductGtinFilter() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .userId(USER_ID)
                .build();

        Map<String,String> additionalProperties = Map.of("productGtin", PRODUCT_GTIN);
        rt1.setAdditionalProperties(additionalProperties);

        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                "REWARDED",
                null,
                null,
                null
        );

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                filters,
                POINT_OF_SALE_ID,
                USER_ID,
                PRODUCT_GTIN,
                OrganizationRole.MERCHANT,
                pageable
        );

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());

        cleanDataPageable();
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
                .initiatives(List.of(INITIATIVE_ID)).build();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .idTrxIssuer("IDTRXISSUER")
                .userId(USER_ID)
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POINT_OF_SALE_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID)).build();

        rewardTransactionRepository.save(rt1).block();
        rewardTransactionRepository.save(rt2).block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                null,
                null,
                null,
                null
        );

        Pageable ascSort = PageRequest.of(0, 10, Sort.by("status"));

        List<RewardTransaction> ascResult = rewardTransactionSpecificRepository.findByFilterTrx(
                filters,
                POINT_OF_SALE_ID,
                USER_ID,
                "",
                OrganizationRole.MERCHANT,
                ascSort
        ).toStream().toList();

        assertEquals(
                List.of(rt1.getId(), rt2.getId()),
                ascResult.stream().map(RewardTransaction::getId).toList()
        );

        Pageable descSort = PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "status"));
        List<RewardTransaction> descResult = rewardTransactionSpecificRepository.findByFilterTrx(
                filters,
                POINT_OF_SALE_ID,
                USER_ID,
                "",
                OrganizationRole.MERCHANT,
                descSort
        ).toStream().toList();

        assertEquals(
                List.of(rt2.getId(), rt1.getId()),
                descResult.stream().map(RewardTransaction::getId).toList()
        );

        cleanDataPageable();
    }

    @Test
    void findByFilterTrx_withUpdateDateSorting_shouldWork() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .trxDate(LocalDateTime.now())
                .elaborationDateTime(LocalDateTime.now().plusMinutes(5))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "updateDate"));

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                "REWARDED",
                null,
                null,
                null
        );

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                filters,
                POINT_OF_SALE_ID,
                USER_ID,
                "",
                OrganizationRole.MERCHANT,
                pageable
        );

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.get(0).getId());

        cleanDataPageable();
    }

    @Test
    void findByFilterTrx_withProductNameSorting_shouldMapToAdditionalPropertiesProductName() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .merchantId(MERCHANT_ID)
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID))
                .trxDate(LocalDateTime.now())
                .elaborationDateTime(LocalDateTime.now().plusMinutes(5))
                .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "productName"));

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                "REWARDED",
                null,
                null,
                null
        );

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
                filters,
                POINT_OF_SALE_ID,
                USER_ID,
                "",
                OrganizationRole.MERCHANT,
                pageable
        );

        List<RewardTransaction> list = result.toStream().toList();

        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.get(0).getId());

        cleanDataPageable();
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

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                null,
                null,
                null,
                null
        );

        Mono<Long> count = rewardTransactionSpecificRepository.getCount(
                filters,
                POINT_OF_SALE_ID,
                null,
                null,
                OrganizationRole.MERCHANT
        );

        assertEquals(1L, count.block());

        cleanDataPageable();
    }

    @Test
    void findOneByInitiativeId() {
        rt = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .status("REWARDED")
                .initiatives(List.of("INITIATIVEID0")).build();
        rewardTransactionRepository.save(rt).block();
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id2")
                .idTrxIssuer("IDTRXISSUER")
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt1).block();
        rt2 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id3")
                .idTrxIssuer("IDTRXISSUER")
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt2).block();

        RewardTransaction trx = rewardTransactionSpecificRepository.findOneByInitiativeId(INITIATIVE_ID).block();
        assertNotNull(trx);
        assertTrue(trx.getInitiatives().contains(INITIATIVE_ID));

        cleanDataPageable();
    }

    @Test
    void findByInitiativesWithBatch() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt1).block();

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByInitiativesWithBatch(INITIATIVE_ID, 100);

        List<RewardTransaction> rewardTransactions = result.toStream().toList();
        assertEquals(1, rewardTransactions.size());

        cleanDataPageable();
    }

    @Test
    void removeInitiativeOnTransaction() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt1).block();

        rewardTransactionSpecificRepository.removeInitiativeOnTransaction(rt1.getId(), INITIATIVE_ID).block();

        RewardTransaction modifiedTrx = rewardTransactionRepository.findById(rt1.getId()).block();
        assertTrue(modifiedTrx.getInitiatives().isEmpty());

        cleanDataPageable();
    }

    @Test
    void findTransaction_shouldReturnMatchingTransaction_whenStatusIsRewardedOrRefunded() {
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

        cleanDataPageable();
    }

    @Test
    void findByTrxIdAndUserId_shouldReturnMatchingTransaction() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .userId("TESTUSER")
            .merchantId(MERCHANT_ID)
            .status("REWARDED")
            .build();
        rewardTransactionRepository.save(rt1).block();

        RewardTransaction found = rewardTransactionSpecificRepository
            .findByTrxIdAndUserId("id1", "TESTUSER")
            .block();

        assertNotNull(found);
        assertEquals("id1", found.getId());
        assertEquals("TESTUSER", found.getUserId());

        RewardTransaction wrongUser = rewardTransactionSpecificRepository
            .findByTrxIdAndUserId("id1", "OTHERUSER")
            .block();
        assertNull(wrongUser);

        RewardTransaction wrongTrx = rewardTransactionSpecificRepository
            .findByTrxIdAndUserId("WRONGID", "TESTUSER")
            .block();
        assertNull(wrongTrx);

        rewardTransactionRepository.deleteById("id1").block();
    }

    @Test
    void rewardTransactionsByBatchId_shouldUpdateAllMatchingTransactions() {
        String batchId = "BATCH123";

        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .rewardBatchId(batchId)
            .status("INVOICED")
            .build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
            .id("id2")
            .rewardBatchId(batchId)
            .status("INVOICED")
            .build();
        rewardTransactionRepository.save(rt2).block();

        rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
            .id("id3")
            .rewardBatchId("OTHERBATCH")
            .status("INVOICED")
            .build();
        rewardTransactionRepository.save(rt3).block();

        rewardTransactionSpecificRepository.rewardTransactionsByBatchId(batchId);

        RewardTransaction updated1 = rewardTransactionRepository.findById("id1").block();
        RewardTransaction updated2 = rewardTransactionRepository.findById("id2").block();
        RewardTransaction unchanged3 = rewardTransactionRepository.findById("id3").block();

        assertNotNull(updated1);
        assertEquals("REWARDED", updated1.getStatus());
        assertNotNull(updated2);
        assertEquals("REWARDED", updated2.getStatus());
        assertNotNull(unchanged3);
        assertNotEquals("REWARDED", unchanged3.getStatus());

        rewardTransactionRepository.deleteById("id1").block();
        rewardTransactionRepository.deleteById("id2").block();
        rewardTransactionRepository.deleteById("id3").block();
    }
}
