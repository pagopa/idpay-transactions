package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
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
import reactor.test.StepVerifier;

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
                rewardTransactionSpecificRepository.findByFilter(filters, USER_ID, false, paging);

        List<RewardTransaction> result = transactionInProgressList.toStream().toList();
        assertEquals(1, result.size());
        assertEquals(rt1, result.getFirst());

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
                false,
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
                false,
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
                false,
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
                false,
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
                false,
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
                false,
                pageable
        );

        List<RewardTransaction> list = result.toStream().toList();
        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());

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
                false,
                pageable
        );

        List<RewardTransaction> list = result.toStream().toList();

        assertEquals(1, list.size());
        assertEquals(rt1.getId(), list.getFirst().getId());

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
                false
        );

        assertEquals(1L, count.block());

        cleanDataPageable();
    }

    /**
     * Test specifico sulla nuova logica includeToCheckWithConsultable:
     * - quando includeToCheckWithConsultable = false, CONSULTABLE filtra solo CONSULTABLE
     * - quando includeToCheckWithConsultable = true, CONSULTABLE filtra CONSULTABLE + TO_CHECK
     */
    @Test
    void findByFilter_withIncludeToCheckWithConsultableFlag() {
        String batchId = "BATCH_TEST";

        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .merchantId(MERCHANT_ID)
                .status("INVOICED")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .initiatives(List.of(INITIATIVE_ID))
                .userId(USER_ID)
                .build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .merchantId(MERCHANT_ID)
                .status("INVOICED")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .initiatives(List.of(INITIATIVE_ID))
                .userId(USER_ID)
                .build();
        rewardTransactionRepository.save(rt2).block();

        Pageable paging = PageRequest.of(0, 10);

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                null,
                batchId,
                RewardBatchTrxStatus.CONSULTABLE,
                null
        );

        List<RewardTransaction> onlyConsultable = rewardTransactionSpecificRepository.findByFilter(
                filters,
                USER_ID,
                false,
                paging
        ).toStream().toList();

        assertEquals(1, onlyConsultable.size());
        assertEquals(rt1.getId(), onlyConsultable.getFirst().getId());

        List<RewardTransaction> consultableAndToCheck = rewardTransactionSpecificRepository.findByFilter(
                filters,
                USER_ID,
                true,
                paging
        ).toStream().toList();

        assertEquals(2, consultableAndToCheck.size());
        List<String> ids = consultableAndToCheck.stream().map(RewardTransaction::getId).toList();
        assertTrue(ids.contains(rt1.getId()));
        assertTrue(ids.contains(rt2.getId()));

        rewardTransactionRepository.deleteById("id1").block();
        rewardTransactionRepository.deleteById("id2").block();
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
    void findByInitiativeIdAndUserId_shouldReturnMatchingTransaction() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .userId("TESTUSER")
            .merchantId(MERCHANT_ID)
            .status("REWARDED")
            .initiativeId("id1")
            .initiatives(List.of("id1"))
            .build();
        rewardTransactionRepository.save(rt1).block();

        RewardTransaction found = rewardTransactionSpecificRepository
            .findByInitiativeIdAndUserId("id1","TESTUSER")
                .blockFirst();

        assertNotNull(found);
        assertEquals("id1", found.getId());
        assertEquals("TESTUSER", found.getUserId());

        RewardTransaction wrongUser = rewardTransactionSpecificRepository
            .findByInitiativeIdAndUserId( "id1","OTHERUSER")
            .blockFirst();
        assertNull(wrongUser);

        RewardTransaction wrongTrx = rewardTransactionSpecificRepository
            .findByInitiativeIdAndUserId("TESTUSER","WRONGID")
            .blockFirst();
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
                .samplingKey(1)
                .build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .rewardBatchId(batchId)
                .status("INVOICED")
                .samplingKey(1)
                .build();
        rewardTransactionRepository.save(rt2).block();

        rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("id3")
                .rewardBatchId("OTHERBATCH")
                .status("INVOICED")
                .samplingKey(3)
                .build();
        rewardTransactionRepository.save(rt3).block();

        rewardTransactionSpecificRepository.rewardTransactionsByBatchId(batchId).block();

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

    @Test
    void sumSuspendedAccruedRewardCents_shouldReturnCorrectSum() {
        String rewardBatchId = "BATCH123";
        String initiativeId = INITIATIVE_ID;

        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .rewardBatchId(rewardBatchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(Map.of(initiativeId, Reward.builder()
                        .accruedRewardCents(1000L)
                        .build()))
                .build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .rewardBatchId(rewardBatchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(Map.of(initiativeId, Reward.builder()
                        .accruedRewardCents(2000L)
                        .build()))
                .build();
        rewardTransactionRepository.save(rt2).block();

        rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("id3")
                .rewardBatchId(rewardBatchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(initiativeId, Reward.builder()
                        .accruedRewardCents(5000L)
                        .build()))
                .build();
        rewardTransactionRepository.save(rt3).block();

        Mono<Long> resultMono = rewardTransactionSpecificRepository
                .sumSuspendedAccruedRewardCents(rewardBatchId);

        Long result = resultMono.block();
        assertNotNull(result);
        assertEquals(1000L + 2000L, result);

        rewardTransactionRepository.deleteById("id1").block();
        rewardTransactionRepository.deleteById("id2").block();
        rewardTransactionRepository.deleteById("id3").block();
    }

    @Test
    void updateStatusAndReturnOld() {
        String trxSuspendedId = "TRX_SUSPENDED_ID";
        String batchId = "BATCH_ID";
        String batchMonth = "dicembre 2025";
        RewardTransaction trxToSave = RewardTransaction.builder()
                .id(trxSuspendedId)
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).build();

        rewardTransactionRepository.save(trxToSave).block();

        RewardTransaction result = rewardTransactionRepository.updateStatusAndReturnOld(batchId, trxSuspendedId, RewardBatchTrxStatus.APPROVED, null, batchMonth).block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(RewardBatchTrxStatus.SUSPENDED, result.getRewardBatchTrxStatus());

        RewardTransaction afterUpdate = rewardTransactionRepository.findById(trxSuspendedId).block();
        Assertions.assertNotNull(afterUpdate);
        Assertions.assertEquals(RewardBatchTrxStatus.APPROVED, afterUpdate.getRewardBatchTrxStatus());
        Assertions.assertEquals(batchMonth, afterUpdate.getRewardBatchLastMonthElaborated());

        rewardTransactionRepository.deleteById(trxToSave.getId()).block();

    }

    @Test
    void updateStatusAndReturnOld_ApprovedTrx() {
        String trxSuspendedId = "TRX_APPROVED_ID";
        String batchId = "BATCH_ID";
        String batchMonth = "dicembre 2025";

        RewardTransaction trxToSave = RewardTransaction.builder()
                .id(trxSuspendedId)
                .rewardBatchId(batchId)
                .rewardBatchRejectionReason("TEST")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED).build();

        rewardTransactionRepository.save(trxToSave).block();

        RewardTransaction result = rewardTransactionRepository.updateStatusAndReturnOld(batchId, trxSuspendedId, RewardBatchTrxStatus.APPROVED, null, batchMonth).block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(RewardBatchTrxStatus.SUSPENDED, result.getRewardBatchTrxStatus());

        RewardTransaction afterUpdate = rewardTransactionRepository.findById(trxSuspendedId).block();
        Assertions.assertNotNull(afterUpdate);
        Assertions.assertEquals(RewardBatchTrxStatus.APPROVED, afterUpdate.getRewardBatchTrxStatus());
        Assertions.assertNull(afterUpdate.getRewardBatchRejectionReason());
        Assertions.assertEquals(batchMonth, afterUpdate.getRewardBatchLastMonthElaborated());

        rewardTransactionRepository.deleteById(trxToSave.getId()).block();

    }

    @Test
    void updateStatusAndReturnOld_ApprovedTrxAlreadyApprove() {
        String trxSuspendedId = "TRX_APPROVED_ID";
        String batchId = "BATCH_ID";
        String batchMonth = "dicembre 2025";

        RewardTransaction trxToSave = RewardTransaction.builder()
                .id(trxSuspendedId)
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).build();

        rewardTransactionRepository.save(trxToSave).block();

        RewardTransaction result = rewardTransactionRepository.updateStatusAndReturnOld(batchId, trxSuspendedId, RewardBatchTrxStatus.APPROVED, null, batchMonth).block();
        Assertions.assertNotNull(result);

        RewardTransaction afterUpdate = rewardTransactionRepository.findById(trxSuspendedId).block();
        Assertions.assertNotNull(afterUpdate);
        Assertions.assertEquals(RewardBatchTrxStatus.APPROVED, afterUpdate.getRewardBatchTrxStatus());
        Assertions.assertEquals(batchMonth, afterUpdate.getRewardBatchLastMonthElaborated());

        rewardTransactionRepository.deleteById(trxToSave.getId()).block();

    }

    @Test
    void updateStatusAndReturnOld_forTrxInAnotherBatch() {
        String trxSuspendedId = "TRX_APPROVED_ID";
        String batchId = "BATCH_ID";
        String batchMonth = "dicembre 2025";

        RewardTransaction trxToSave = RewardTransaction.builder()
                .id(trxSuspendedId)
                .rewardBatchId("BATCH_ID_2")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).build();

        rewardTransactionRepository.save(trxToSave).block();

        RewardTransaction result = rewardTransactionRepository.updateStatusAndReturnOld(batchId, trxSuspendedId, RewardBatchTrxStatus.APPROVED, null, batchMonth).block();
        Assertions.assertNull(result);

        RewardTransaction afterUpdate = rewardTransactionRepository.findById(trxSuspendedId).block();
        Assertions.assertNotNull(afterUpdate);
        Assertions.assertEquals(RewardBatchTrxStatus.APPROVED, afterUpdate.getRewardBatchTrxStatus());
        Assertions.assertNull(afterUpdate.getRewardBatchLastMonthElaborated());

        rewardTransactionRepository.deleteById(trxToSave.getId()).block();

    }

    @Test
    void updateStatusAndReturnOld_RejectedTrxAlreadyApprove() {
        String trxSuspendedId = "TRX_APPROVED_ID";
        String batchId = "BATCH_ID";
        String reason = "REASON_TEST";
        String batchMonth = "dicembre 2025";

        RewardTransaction trxToSave = RewardTransaction.builder()
                .id(trxSuspendedId)
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED).build();

        rewardTransactionRepository.save(trxToSave).block();

        RewardTransaction result = rewardTransactionRepository.updateStatusAndReturnOld(batchId, trxSuspendedId, RewardBatchTrxStatus.REJECTED, reason, batchMonth).block();
        Assertions.assertNotNull(result);

        RewardTransaction afterUpdate = rewardTransactionRepository.findById(trxSuspendedId).block();
        Assertions.assertNotNull(afterUpdate);
        Assertions.assertEquals(RewardBatchTrxStatus.REJECTED, afterUpdate.getRewardBatchTrxStatus());
        Assertions.assertEquals(reason, afterUpdate.getRewardBatchRejectionReason());
        Assertions.assertEquals(batchMonth, afterUpdate.getRewardBatchLastMonthElaborated());

        rewardTransactionRepository.deleteById(trxToSave.getId()).block();

    }

    @Test
    void findByFilter_trxBatchStatus() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .initiatives(List.of(INITIATIVE_ID))
                .rewardBatchId("batchId")
                .status("INVOICED")
                .samplingKey(1)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .build();
        rewardTransactionRepository.save(rt1).block();

        RewardTransaction result = rewardTransactionRepository.findByFilter(rt1.getRewardBatchId(), INITIATIVE_ID, List.of(RewardBatchTrxStatus.TO_CHECK)).blockFirst();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(rt1.getId(), result.getId());

        rewardTransactionRepository.deleteById(rt1.getId()).block();

    }

    @Test
    void findInvoicedTransactionsWithoutBatch_returnsOnlyMatchingTransactions() {

        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .rewardBatchId(null)
                .status("INVOICED")
                .build();
        rewardTransactionRepository.save(rt1).block();

        rt2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("id2")
                .rewardBatchId("BATCH1")
                .status("INVOICED")
                .build();
        rewardTransactionRepository.save(rt2).block();

        rt3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("id3")
                .rewardBatchId(null)
                .status("CANCELLED")
                .build();
        rewardTransactionRepository.save(rt3).block();

        Flux<RewardTransaction> result = rewardTransactionSpecificRepository.findInvoicedTransactionsWithoutBatch(10);

        List<RewardTransaction> list = result.collectList().block();
        Assertions.assertNotNull(list);
        Assertions.assertEquals(1, list.size());
        Assertions.assertEquals("id1", list.getFirst().getId());
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_returnsTransaction_whenMatching() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .status("INVOICED")
                .rewardBatchId(null)
                .build();
        rewardTransactionRepository.save(rt1).block();

        Mono<RewardTransaction> resultMono = rewardTransactionSpecificRepository.findInvoicedTrxByIdWithoutBatch("id1");

        RewardTransaction result = resultMono.block();
        Assertions.assertNotNull(result);
        Assertions.assertEquals("id1", result.getId());
        Assertions.assertEquals("INVOICED", result.getStatus());
        Assertions.assertNull(result.getRewardBatchId());

        rewardTransactionRepository.deleteById(rt1.getId()).block();
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_returnsEmpty_whenNotMatching() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .status("CANCELLED")
                .rewardBatchId(null)
                .build();
        rewardTransactionRepository.save(rt1).block();

        Mono<RewardTransaction> resultMono = rewardTransactionSpecificRepository.findInvoicedTrxByIdWithoutBatch("id1");

        StepVerifier.create(resultMono)
                .expectNextCount(0)
                .verifyComplete();

        rewardTransactionRepository.deleteById(rt1.getId()).block();
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_returnsEmpty_whenRewardBatchIdNotNull() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("id1")
                .status("INVOICED")
                .rewardBatchId("BATCH1")
                .build();
        rewardTransactionRepository.save(rt1).block();

        Mono<RewardTransaction> resultMono = rewardTransactionSpecificRepository.findInvoicedTrxByIdWithoutBatch("id1");

        StepVerifier.create(resultMono)
                .expectNextCount(0)
                .verifyComplete();

        rewardTransactionRepository.deleteById(rt1.getId()).block();
    }

    @Test
    void findDistinctFranchiseAndPosByRewardBatchId() {

      String rewardBatchId = "BATCH_TEST";

      RewardTransaction trx1 = RewardTransactionFaker.mockInstanceBuilder(1)
          .id("trx1")
          .rewardBatchId(rewardBatchId)
          .franchiseName("FranchiseA")
          .pointOfSaleId("POS1")
          .build();

      RewardTransaction trx2 = RewardTransactionFaker.mockInstanceBuilder(2)
          .id("trx2")
          .rewardBatchId(rewardBatchId)
          .franchiseName("FranchiseA")
          .pointOfSaleId("POS1")
          .build();

      RewardTransaction trx3 = RewardTransactionFaker.mockInstanceBuilder(3)
          .id("trx3")
          .rewardBatchId(rewardBatchId)
          .franchiseName("FranchiseA")
          .pointOfSaleId("POS2")
          .build();

      RewardTransaction trx4 = RewardTransactionFaker.mockInstanceBuilder(4)
          .id("trx4")
          .rewardBatchId("OTHER_BATCH")
          .franchiseName("FranchiseA")
          .pointOfSaleId("POS9")
          .build();

      rewardTransactionRepository.saveAll(List.of(trx1, trx2, trx3, trx4)).collectList().block();

      List<FranchisePointOfSaleDTO> result = rewardTransactionSpecificRepository
          .findDistinctFranchiseAndPosByRewardBatchId(rewardBatchId)
          .toStream()
          .toList();

      assertEquals(2, result.size());

      assertTrue(result.stream().anyMatch(r ->
          "FranchiseA".equals(r.getFranchiseName()) &&
              "POS1".equals(r.getPointOfSaleId())
      ));

      assertTrue(result.stream().anyMatch(r ->
          "FranchiseA".equals(r.getFranchiseName()) &&
              "POS2".equals(r.getPointOfSaleId())
      ));

      rewardTransactionRepository.deleteAll(List.of(trx1, trx2, trx3, trx4)).block();

    }

    @Test
    void findTransactionInBatch_returnsEmpty_whenRewardBatchIdDoesNotMatch() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .merchantId(MERCHANT_ID)
            .rewardBatchId("BATCH1")
            .build();

        rewardTransactionRepository.save(rt1).block();

        RewardTransaction result =
            rewardTransactionSpecificRepository
                .findTransactionInBatch(MERCHANT_ID, "BATCH2", "id1")
                .block();

        Assertions.assertNull(result);

        rewardTransactionRepository.deleteById(rt1.getId()).block();
    }

    @Test
    void findTransactionInBatch_returnsEmpty_whenTransactionIdDoesNotMatch() {
        rt1 = RewardTransactionFaker.mockInstanceBuilder(1)
            .id("id1")
            .merchantId(MERCHANT_ID)
            .rewardBatchId("BATCH1")
            .build();

        rewardTransactionRepository.save(rt1).block();

        RewardTransaction result =
            rewardTransactionSpecificRepository
                .findTransactionInBatch(MERCHANT_ID, "BATCH1", "OTHER_TRX")
                .block();

        Assertions.assertNull(result);

        rewardTransactionRepository.deleteById(rt1.getId()).block();
    }
}
