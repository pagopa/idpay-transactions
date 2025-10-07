package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.model.counters.RewardCounters;
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
import java.util.HashMap;
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
                .amountCents(amountCents).build();
        rewardTransactionRepository.save(rt).block();
    }

    @AfterEach
    void clearData(){
        rewardTransactionRepository.deleteById("id_prova").block();
    }

    @Test
    void findByIdTrxIssuer() {
        Flux<RewardTransaction> resultTrxIssuer = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,null, null, null);
        Assertions.assertNotNull(resultTrxIssuer);
        List<RewardTransaction> rewardTransactionsList = resultTrxIssuer.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList.size());
        Assertions.assertEquals(rewardTransactionsList, List.of(rt));
    }

    @Test
    void findByIdTrxIssuerAndOptionalFilters() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultTrxIssuerAndUserId = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),rt.getUserId() ,null,null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndUserId);
        List<RewardTransaction> resultTrxIssuerAndUserIdList = resultTrxIssuerAndUserId.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndUserIdList.size());
        Assertions.assertEquals(resultTrxIssuerAndUserIdList, List.of(rt));

        Flux<RewardTransaction> resultTrxIssuerAndStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,startDate,null, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndStartDate);
        List<RewardTransaction> resultTrxIssuerAndStartDateList = resultTrxIssuerAndStartDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndStartDateList.size());
        Assertions.assertEquals(resultTrxIssuerAndStartDateList, List.of(rt));

        Flux<RewardTransaction> resultTrxIssuerAndEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,endDate, null, null);
        Assertions.assertNotNull(resultTrxIssuerAndEndDate);
        List<RewardTransaction> resultTrxIssuerAndEndDateList = resultTrxIssuerAndEndDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndEndDateList.size());
        Assertions.assertEquals(resultTrxIssuerAndEndDateList, List.of(rt));


        Flux<RewardTransaction> resultTrxIssuerAndAmount = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,null, rt.getAmountCents(), null);
        Assertions.assertNotNull(resultTrxIssuerAndAmount);
        List<RewardTransaction> resultTrxIssuerAndAmountList = resultTrxIssuerAndAmount.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndAmountList.size());
        Assertions.assertEquals(resultTrxIssuerAndAmountList, List.of(rt));

        Flux<RewardTransaction> resultTrxIssuerAndRangeDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,startDate,endDate, rt.getAmountCents(), null);
        Assertions.assertNotNull(resultTrxIssuerAndRangeDate);
        List<RewardTransaction> resultTrxIssuerAndRangeDateList = resultTrxIssuerAndRangeDate.toStream().toList();
        Assertions.assertEquals(1, resultTrxIssuerAndRangeDateList.size());
        Assertions.assertEquals(resultTrxIssuerAndRangeDateList, List.of(rt));

        Flux<RewardTransaction> resultBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,rt.getTrxDate().plusDays(10L),null, null, null);
        Assertions.assertNotNull(resultBeforeStartDateBeforeStartDate);
        Assertions.assertEquals(0, resultBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultDateAfterEndDate = rewardTransactionSpecificRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(),null ,null,rt.getTrxDate().minusDays(10L), null, null);
        Assertions.assertNotNull(resultDateAfterEndDate);
        Assertions.assertEquals(0, resultDateAfterEndDate.count().block());
    }
    @Test
    void findByUserIdAndRangeDateAndAmount() {
        LocalDateTime startDate = rt.getTrxDate().minusMonths(5L);
        LocalDateTime endDate = rt.getTrxDate().plusMonths(6L);

        Flux<RewardTransaction> resultUserIDAndRangeDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,endDate,null, null);
        Assertions.assertNotNull(resultUserIDAndRangeDate);
        List<RewardTransaction> resultUserIDAndRangeDateList = resultUserIDAndRangeDate.toStream().toList();
        Assertions.assertEquals(1, resultUserIDAndRangeDateList.size());
        Assertions.assertEquals(resultUserIDAndRangeDateList, List.of(rt));

        Flux<RewardTransaction> resultUserIDAndRangeDateAndAmount = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,endDate,rt.getAmountCents(), null);
        Assertions.assertNotNull(resultUserIDAndRangeDateAndAmount);
        List<RewardTransaction> resultUserIDAndRangeDateAndAmountList = resultUserIDAndRangeDateAndAmount.toStream().toList();
        Assertions.assertEquals(1, resultUserIDAndRangeDateAndAmountList.size());
        Assertions.assertEquals(resultUserIDAndRangeDateAndAmountList, List.of(rt));

        Flux<RewardTransaction> resultUserIDBeforeStartDateBeforeStartDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), rt.getTrxDate().plusDays(10L) ,endDate,null, null);
        Assertions.assertNotNull(resultUserIDBeforeStartDateBeforeStartDate);
        Assertions.assertEquals(0, resultUserIDBeforeStartDateBeforeStartDate.count().block());

        Flux<RewardTransaction> resultUserIDDateAfterEndDate = rewardTransactionSpecificRepository.findByRange(rt.getUserId(), startDate ,rt.getTrxDate().minusDays(10L),null, null);
        Assertions.assertNotNull(resultUserIDDateAfterEndDate);
        Assertions.assertEquals(0, resultUserIDDateAfterEndDate.count().block());
    }

    @Test
    void pageableWithfindByIdTrxIssuer(){
        LocalDateTime date = LocalDateTime.of(2021, 9, 6, 17, 30, 25);
        setUpPageable(date, "userId");

        Pageable pageable = PageRequest.of(0,2);
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList.size());
        Assertions.assertEquals(rewardTransactionsList, List.of(rt1, rt2));

        Pageable pageable2 = PageRequest.of(1,2);
        Flux<RewardTransaction> result2 = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable2);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList2.size());
        Assertions.assertEquals(rewardTransactionsList2, List.of(rt3));

        Pageable pageable3 = PageRequest.of(0,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable3);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList3.size());
        Assertions.assertEquals(rewardTransactionsList3, List.of(rt3, rt2));

        Pageable pageable4 = PageRequest.of(1,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 = rewardTransactionSpecificRepository.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, pageable4);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList4.size());
        Assertions.assertEquals(rewardTransactionsList4, List.of(rt1));

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
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList = result.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList.size());
        Assertions.assertEquals(rewardTransactionsList, List.of(rt1, rt2));

        Pageable pageable2 = PageRequest.of(1,2);
        Flux<RewardTransaction> result2 = rewardTransactionSpecificRepository.findByRange( userId,startDate,endDate, null, pageable2);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList2 = result2.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList2.size());
        Assertions.assertEquals(rewardTransactionsList2, List.of(rt3));

        Pageable pageable3 = PageRequest.of(0,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result3 = rewardTransactionSpecificRepository.findByRange(userId,startDate, endDate, null, pageable3);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList3 = result3.toStream().toList();
        Assertions.assertEquals(2, rewardTransactionsList3.size());
        Assertions.assertEquals(rewardTransactionsList3, List.of(rt3, rt2));

        Pageable pageable4 = PageRequest.of(1,2, Sort.Direction.DESC, "_id");
        Flux<RewardTransaction> result4 = rewardTransactionSpecificRepository.findByRange(userId, startDate, endDate, null, pageable4);
        Assertions.assertNotNull(result);
        List<RewardTransaction> rewardTransactionsList4 = result4.toStream().toList();
        Assertions.assertEquals(1, rewardTransactionsList4.size());
        Assertions.assertEquals(rewardTransactionsList4, List.of(rt1));

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
                .status("CANCELLED")
                .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt1).block();

        Pageable paging = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());
        Flux<RewardTransaction> transactionInProgressList = rewardTransactionRepository.findByFilter(MERCHANT_ID, INITIATIVE_ID, USER_ID, "CANCELLED", paging);
        List<RewardTransaction> result = transactionInProgressList.toStream().toList();
        assertEquals(rt1, result.get(0));

        cleanDataPageable();
    }

    @Test
    void findByFilterTrx_withSortedPageable_shouldUseProvidedSorting() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .idTrxIssuer("IDTRXISSUER")
            .status("REWARDED")
            .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt1).block();

        Pageable sorted = PageRequest.of(0, 10, Sort.by("elaborationDateTime").descending());

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
            MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID,"", "REWARDED", sorted);

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
            .status("REWARDED")
            .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt1).block();

        Pageable unsorted = PageRequest.of(0, 10);
        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
            MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", "REWARDED", unsorted);

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());

        cleanDataPageable();
    }

    @Test
    void findByFilterTrx() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .idTrxIssuer("IDTRXISSUER")
            .status("REWARDED")
            .initiatives(List.of(INITIATIVE_ID)).build();

        Map<String,String> additionalProperties = Map.of("productGtin", PRODUCT_GTIN);

        rt1.setAdditionalProperties(additionalProperties);

        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(RewardTransaction.Fields.elaborationDateTime).descending());
        Flux<RewardTransaction> result = rewardTransactionRepository.findByFilterTrx(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, PRODUCT_GTIN, "REWARDED", pageable);
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

        cleanDataPageable();
    }

    @Test
    void getCount() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .idTrxIssuer("IDTRXISSUER")
                .status("REWARDED")
                .initiatives(List.of(INITIATIVE_ID)).build();
        rewardTransactionRepository.save(rt1).block();
        Mono<Long> count = rewardTransactionRepository.getCount(MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, null, null, null);
        assertEquals(1, count.block());

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
        RewardTransaction trx= rewardTransactionRepository.findOneByInitiativeId(INITIATIVE_ID).block();
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

        Flux<RewardTransaction> result = rewardTransactionRepository.findByInitiativesWithBatch(INITIATIVE_ID, 100);

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

        rewardTransactionRepository.removeInitiativeOnTransaction(rt1.getId(), INITIATIVE_ID).block();

        RewardTransaction modifiedTrx = rewardTransactionRepository.findById(rt1.getId()).block();
        assertTrue(modifiedTrx.getInitiatives().isEmpty());
        cleanDataPageable();
    }

    @Test
    void findByFilterTrx_withUpdateDateSorting_shouldMapToElaborationDateTime() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .idTrxIssuer("IDTRXISSUER")
            .status("REWARDED")
            .initiatives(List.of(INITIATIVE_ID))
            .trxDate(LocalDateTime.now())
            .elaborationDateTime(LocalDateTime.now().plusMinutes(5))
            .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "updateDate"));

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
            MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", "REWARDED", pageable);

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
            .status("REWARDED")
            .initiatives(List.of(INITIATIVE_ID))
            .trxDate(LocalDateTime.now())
            .elaborationDateTime(LocalDateTime.now().plusMinutes(5))
            .build();
        rewardTransactionRepository.save(rt1).block();

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "productName"));

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findByFilterTrx(
            MERCHANT_ID, INITIATIVE_ID, POINT_OF_SALE_ID, USER_ID, "", "REWARDED", pageable);

        List<RewardTransaction> list = result.toStream().toList();

        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.get(0).getId());

        cleanDataPageable();
    }

    private static Map<String, Reward> getReward() {
        Map<String, Reward> reward = new HashMap<>();
        RewardCounters counter = RewardCounters.builder()
                .exhaustedBudget(false)
                .initiativeBudgetCents(10000L)
                .build();
        Reward rewardElement = Reward.builder()
                .initiativeId(INITIATIVE_ID)
                .organizationId("ORGANIZATIONID")
                .providedRewardCents(1000L)
                .accruedRewardCents(1000L)
                .capped(false)
                .dailyCapped(false)
                .monthlyCapped(false)
                .yearlyCapped(false)
                .weeklyCapped(false)
                .counters(counter)
                .build();
        reward.put(INITIATIVE_ID, rewardElement);
        return reward;
    }

    private static Map<String, List<String>> getInitiativeRejectionReasons() {
        Map<String, List<String>> initiativeRejectionReasons = new HashMap<>();
        initiativeRejectionReasons.put(INITIATIVE_ID,  List.of("BUDGET_EXHAUSTED"));
        return initiativeRejectionReasons;
    }
}