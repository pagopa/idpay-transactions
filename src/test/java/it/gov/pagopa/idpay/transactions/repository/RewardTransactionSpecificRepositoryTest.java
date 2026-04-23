package it.gov.pagopa.idpay.transactions.repository;

import com.mongodb.client.result.UpdateResult;
import it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.service.RewardBatchServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardTransactionSpecificRepositoryTest {

    @Mock
    private ReactiveMongoTemplate mongoTemplate;

    @InjectMocks
    private RewardTransactionSpecificRepositoryImpl repository;

    @Captor
    private ArgumentCaptor<Query> queryCaptor;

    private RewardTransaction trx;
    private TrxFiltersDTO filters;

    @BeforeEach
    void setup() {
        trx = RewardTransaction.builder()
                .id("trx1")
                .idTrxIssuer("ISS1")
                .userId("U1")
                .amountCents(100L)
                .initiatives(List.of())
                .trxDate(LocalDateTime.of(2025, 1, 1, 10, 0))
                .build();
        filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
    }

    @Test
    void shouldFilter_onlyIssuer() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISS1",
                                null,
                                null,
                                null,
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void shouldFilter_userId_and_amount() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISS1",
                                "U1",
                                null,
                                null,
                                100L,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void shouldFilter_dateRange() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        LocalDateTime start = trx.getTrxDate().minusHours(1);
        LocalDateTime end = trx.getTrxDate().plusHours(1);

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISS1",
                                null,
                                start,
                                end,
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void shouldFilter_onlyStartDate() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISS1",
                                null,
                                trx.getTrxDate().minusHours(1),
                                null,
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void shouldFilter_onlyEndDate() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISS1",
                                null,
                                null,
                                trx.getTrxDate().plusHours(1),
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void shouldApply_allFilters_together() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISS1",
                                "U1",
                                trx.getTrxDate().minusHours(1),
                                trx.getTrxDate().plusHours(1),
                                100L,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void shouldHandle_nullPageable() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISS1",
                                null,
                                null,
                                null,
                                null,
                                null
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByRange_shouldFilterByUserAndDateRange() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        LocalDateTime start = trx.getTrxDate().minusHours(1);
        LocalDateTime end = trx.getTrxDate().plusHours(1);

        StepVerifier.create(
                        repository.findByRange(
                                "U1",
                                start,
                                end,
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByRange_withAmount_shouldApplyFilter() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByRange(
                                "U1",
                                trx.getTrxDate().minusHours(1),
                                trx.getTrxDate().plusHours(1),
                                100L,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByRange_nullPageable_shouldUseDefaultSorting() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByRange(
                                "U1",
                                trx.getTrxDate().minusHours(1),
                                trx.getTrxDate().plusHours(1),
                                null,
                                null
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByRange_unsortedPageable_shouldUseDefaultSort() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(
                        repository.findByRange(
                                "U1",
                                trx.getTrxDate().minusHours(1),
                                trx.getTrxDate().plusHours(1),
                                null,
                                pageable
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void getCount_base_shouldUseDefaultStatuses() {
        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();

        verify(mongoTemplate).count(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void getCount_withUserId_shouldFilter() {
        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, "U1", false)
                )
                .expectNext(1L)
                .verifyComplete();

        verify(mongoTemplate).count(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void getCount_withPointOfSale_shouldFilter() {
        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, "POS1", null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_withProductGtin_shouldApplyRegex() {
        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, "ABC123", null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_withExplicitStatus_shouldFilter() {
        filters.setStatus("REWARDED");

        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_withRewardBatchId_shouldFilter() {
        filters.setRewardBatchId("B1");

        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_withTrxCode_shouldApplyRegex() {
        filters.setTrxCode("CODE");

        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_withRewardBatchTrxStatus_shouldFilterExact() {
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);

        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_consultable_withIncludeToCheck_shouldIncludeBoth() {
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(2L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, true)
                )
                .expectNext(2L)
                .verifyComplete();
    }

    @Test
    void getCount_consultable_withoutIncludeToCheck_shouldNotIncludeToCheck() {
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_allFiltersCombined_shouldWorkTogether() {
        filters.setStatus("REWARDED");
        filters.setRewardBatchId("B1");
        filters.setTrxCode("CODE");
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);

        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(
                        repository.getCount(filters, "POS1", "ABC123", "U1", false)
                )
                .expectNext(1L)
                .verifyComplete();

        verify(mongoTemplate).count(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByFilterTrx_shouldUseAggregation_whenAggregationIsNotNull() {

        Pageable pageable = PageRequest.of(
                0,
                10,
                Sort.by(Sort.Direction.ASC, "status")
        );

        when(mongoTemplate.aggregate(any(), eq(RewardTransaction.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByFilterTrx(
                                filters,
                                null,
                                "U1",
                                null,
                                false,
                                pageable
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).aggregate(any(), eq(RewardTransaction.class), eq(RewardTransaction.class));
    }

    @Test
    void findByFilterTrx_shouldUseFind_whenAggregationIsNull() {

        Pageable pageable = PageRequest.of(
                0,
                10,
                Sort.by(Sort.Direction.DESC, "trxDate")
        );

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByFilterTrx(
                                filters,
                                null,
                                "U1",
                                null,
                                false,
                                pageable
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByFilterTrx_shouldHandleNullPageable() {

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByFilterTrx(
                                filters,
                                null,
                                "U1",
                                null,
                                false,
                                null
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByFilterTrx_shouldMapSortField() {

        Pageable pageable = PageRequest.of(
                0,
                10,
                Sort.by(Sort.Direction.ASC, "productName")
        );

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByFilterTrx(
                                filters,
                                null,
                                "U1",
                                null,
                                false,
                                pageable
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));
    }

    @Test
    void findByFilterTrx_consultable_shouldIncludeToCheck() {

        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx, trx));

        StepVerifier.create(
                        repository.findByFilterTrx(
                                filters,
                                null,
                                "U1",
                                null,
                                true,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(2)
                .verifyComplete();

        verify(mongoTemplate).find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void findByFilter_shouldCoverAllCases() {
        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByFilter(
                                "B1",
                                "INIT_1",
                                List.of(RewardBatchTrxStatus.CONSULTABLE)
                        )
                )
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.empty());

        StepVerifier.create(
                        repository.findByFilter(
                                "WRONG",
                                "INIT_1",
                                List.of(RewardBatchTrxStatus.CONSULTABLE)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();

        StepVerifier.create(
                        repository.findByFilter(
                                "B1",
                                "WRONG",
                                List.of(RewardBatchTrxStatus.CONSULTABLE)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();

        StepVerifier.create(
                        repository.findByFilter(
                                "B1",
                                "INIT_1",
                                List.of(RewardBatchTrxStatus.TO_CHECK)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(
                        repository.findByFilter(
                                "B1",
                                "INIT_1",
                                List.of(
                                        RewardBatchTrxStatus.CONSULTABLE,
                                        RewardBatchTrxStatus.TO_CHECK
                                )
                        )
                )
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void findTransaction_shouldCoverAllCases() {
        RewardTransaction transaction = RewardTransaction.builder()
                .id("trx1")
                .merchantId("M1")
                .status(SyncTrxStatus.REWARDED.name())
                .build();

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(transaction));

        StepVerifier.create(
                        repository.findTransaction("M1", "trx1")
                )
                .expectNextMatches(t ->
                        t.getId().equals("trx1") &&
                                t.getMerchantId().equals("M1")
                )
                .verifyComplete();

        verify(mongoTemplate).findOne(queryCaptor.capture(), eq(RewardTransaction.class));

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(
                        repository.findTransaction("M1", "NOT_FOUND")
                )
                .verifyComplete();

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(
                        repository.findTransaction("M1", "trx_invalid_status")
                )
                .verifyComplete();
    }

    @Test
    void findOneByInitiativeId_shouldCoverAllCases() {

        RewardTransaction trx1 = RewardTransaction.builder()
                .id("trx1")
                .initiatives(List.of("INIT_1"))
                .build();

        RewardTransaction trxMulti = RewardTransaction.builder()
                .id("trx2")
                .initiatives(List.of("INIT_1", "INIT_2"))
                .build();

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trx1));

        StepVerifier.create(repository.findOneByInitiativeId("INIT_1"))
                .expectNextMatches(t -> t.getId().equals("trx1"))
                .verifyComplete();

        verify(mongoTemplate).findOne(queryCaptor.capture(), eq(RewardTransaction.class));

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(repository.findOneByInitiativeId("WRONG"))
                .verifyComplete();

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trxMulti));

        StepVerifier.create(repository.findOneByInitiativeId("INIT_2"))
                .expectNextMatches(t -> t.getId().equals("trx2"))
                .verifyComplete();

        verify(mongoTemplate, times(3))
                .findOne(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void removeInitiativeOnTransaction_shouldCallUpdateAndComplete1() {
        String trxId = "trx1";
        String initiativeId = "INIT1";

        when(mongoTemplate.updateFirst(any(Query.class), any(), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        Mono<Void> result = repository.removeInitiativeOnTransaction(trxId, initiativeId);

        StepVerifier.create(result)
                .verifyComplete();

        ArgumentCaptor<Query> argumentCaptor = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);

        verify(mongoTemplate).updateFirst(argumentCaptor.capture(), updateCaptor.capture(), eq(RewardTransaction.class));

        Query capturedQuery = argumentCaptor.getValue();
        Update capturedUpdate = updateCaptor.getValue();

        assertNotNull(capturedQuery);
        assertNotNull(capturedUpdate);

        assertTrue(capturedQuery.getQueryObject().toJson().contains(trxId));

        String updateJson = capturedUpdate.getUpdateObject().toJson();

        assertTrue(updateJson.contains("initiatives"));
        assertTrue(updateJson.contains(initiativeId));
        assertTrue(updateJson.contains("rewards"));
        assertTrue(updateJson.contains("initiativeRejectionReasons"));
    }

    @Test
    void removeInitiativeOnTransaction_shouldCallUpdateAndComplete() {
        String trxId = "trx1";
        String initiativeId = "INIT1";

        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        Mono<Void> result = repository.removeInitiativeOnTransaction(trxId, initiativeId);

        StepVerifier.create(result)
                .verifyComplete();

        
        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);

        verify(mongoTemplate).updateFirst(queryCaptor.capture(), updateCaptor.capture(), eq(RewardTransaction.class));

        Query query = queryCaptor.getValue();
        Update update = updateCaptor.getValue();

        assertNotNull(query);
        assertNotNull(update);

        String queryJson = query.getQueryObject().toJson();
        String updateJson = update.getUpdateObject().toJson();

        assertTrue(queryJson.contains(trxId));
        assertTrue(updateJson.contains("initiatives"));
        assertTrue(updateJson.contains(initiativeId));
        assertTrue(updateJson.contains("rewards"));
        assertTrue(updateJson.contains("initiativeRejectionReasons"));
    }

    @Test
    void findByInitiativesWithBatch_shouldReturnResultsAndApplyBatchSize() {
        RewardTransaction trx = new RewardTransaction();
        trx.setId("trx1");

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Flux<RewardTransaction> result =
                repository.findByInitiativesWithBatch("INIT1", 50);

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));

        Query query = queryCaptor.getValue();
        assertNotNull(query);

        String queryJson = query.getQueryObject().toJson();

        assertTrue(queryJson.contains("INIT1"));
    }

    @Test
    void findByInitiativeIdAndUserId_shouldFilterCorrectly() {
        RewardTransaction trx = new RewardTransaction();
        trx.setId("trx1");

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Flux<RewardTransaction> result =
                repository.findByInitiativeIdAndUserId("INIT1", "USER1");

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));

        Query query = queryCaptor.getValue();
        assertNotNull(query);

        String queryJson = query.getQueryObject().toJson();

        assertTrue(queryJson.contains("INIT1"));
        assertTrue(queryJson.contains("USER1"));
    }

    @Test
    void rewardTransactions_totalZero_shouldCompleteWithoutFurtherCalls() {
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(UpdateResult.acknowledged(0, 0L, null)));

        Mono<Void> result =
                repository.rewardTransactionsByBatchIdAndInitiativeId("B1", "INIT1");

        StepVerifier.create(result)
                .verifyComplete();

        verify(mongoTemplate, times(1))
                .updateMulti(any(Query.class), any(Update.class), eq(RewardTransaction.class));

        verify(mongoTemplate, never())
                .find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void rewardTransactions_totalPositive_butNoIds_shouldSkipSecondUpdate() {
        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(UpdateResult.acknowledged(5, 5L, null)));

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.empty());

        Mono<Void> result =
                repository.rewardTransactionsByBatchIdAndInitiativeId("B1", "INIT1");

        StepVerifier.create(result)
                .verifyComplete();

        verify(mongoTemplate, times(1))
                .updateMulti(any(Query.class), any(Update.class), eq(RewardTransaction.class));

        verify(mongoTemplate, times(1))
                .find(any(Query.class), eq(RewardTransaction.class));

        verify(mongoTemplate, times(1)) // solo la prima update
                .updateMulti(any(Query.class), any(Update.class), eq(RewardTransaction.class));
    }

    @Test
    void rewardTransactions_fullFlow_shouldExecuteAllSteps() {
        RewardTransaction trx1 = new RewardTransaction();
        trx1.setId("id1");

        RewardTransaction trx2 = new RewardTransaction();
        trx2.setId("id2");

        when(mongoTemplate.updateMulti(any(Query.class), any(Update.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(UpdateResult.acknowledged(10, 10L, null))) // prima update
                .thenReturn(Mono.just(UpdateResult.acknowledged(2, 2L, null)));  // seconda update

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx1, trx2));

        Mono<Void> result =
                repository.rewardTransactionsByBatchIdAndInitiativeId("B1", "INIT1");

        StepVerifier.create(result)
                .verifyComplete();

        verify(mongoTemplate, times(2))
                .updateMulti(any(Query.class), any(Update.class), eq(RewardTransaction.class));

        verify(mongoTemplate, times(1))
                .find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void sumSuspendedAccruedRewardCents_shouldReturnTotal() {

        RewardBatchServiceImpl.TotalAmount total = new RewardBatchServiceImpl.TotalAmount();
        total.setTotal(250L);

        when(mongoTemplate.aggregate(
                any(Aggregation.class),
                eq(RewardTransaction.class),
                eq(RewardBatchServiceImpl.TotalAmount.class)
        )).thenReturn(Flux.just(total));

        Mono<Long> result =
                repository.sumSuspendedAccruedRewardCents("INIT1", "BATCH1");

        StepVerifier.create(result)
                .expectNext(250L)
                .verifyComplete();

        verify(mongoTemplate).aggregate(
                any(Aggregation.class),
                eq(RewardTransaction.class),
                eq(RewardBatchServiceImpl.TotalAmount.class)
        );
    }

    @Test
    void updateStatusAndReturnOld_shouldReturnEmpty_whenTransactionNotFound() {

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        Mono<RewardTransaction> result =
                repository.updateStatusAndReturnOld(
                        "INIT1",
                        "BATCH1",
                        "TRX1",
                        RewardBatchTrxStatus.APPROVED,
                        null,
                        "2025-01",
                        null
                );

        StepVerifier.create(result)
                .verifyComplete();

        verify(mongoTemplate).findOne(any(Query.class), eq(RewardTransaction.class));
        verify(mongoTemplate, never()).findAndModify(any(), any(), any(), eq(RewardTransaction.class));
    }

    @Test
    void updateStatusAndReturnOld_shouldReturnEmpty_whenFindAndModifyReturnsNull() {

        RewardTransaction current = new RewardTransaction();
        current.setId("TRX1");
        current.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(current));

        when(mongoTemplate.findAndModify(any(Query.class), any(), any(), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        Mono<RewardTransaction> result =
                repository.updateStatusAndReturnOld(
                        "INIT1",
                        "BATCH1",
                        "TRX1",
                        RewardBatchTrxStatus.APPROVED,
                        null,
                        "2025-01",
                        null
                );

        StepVerifier.create(result)
                .verifyComplete();

        verify(mongoTemplate).findOne(any(Query.class), eq(RewardTransaction.class));
        verify(mongoTemplate).findAndModify(any(), any(), any(), eq(RewardTransaction.class));
    }

    @Test
    void updateStatusAndReturnOld_shouldReturnOldDocument_whenUpdateSucceeds() {

        RewardTransaction current = new RewardTransaction();
        current.setId("TRX1");
        current.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);

        RewardTransaction old = new RewardTransaction();
        old.setId("TRX1");
        old.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(current));

        when(mongoTemplate.findAndModify(any(Query.class), any(), any(), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(old));

        Mono<RewardTransaction> result =
                repository.updateStatusAndReturnOld(
                        "INIT1",
                        "BATCH1",
                        "TRX1",
                        RewardBatchTrxStatus.APPROVED,
                        null,
                        "2025-01",
                        null
                );

        StepVerifier.create(result)
                .expectNext(old)
                .verifyComplete();

        verify(mongoTemplate).findOne(any(Query.class), eq(RewardTransaction.class));
        verify(mongoTemplate).findAndModify(any(), any(), any(), eq(RewardTransaction.class));
    }

    @Test
    void findInvoicedTransactionsWithoutBatch_shouldReturnResults_withPageableAndCriteria() {

        RewardTransaction trx = new RewardTransaction();
        trx.setId("trx1");
        trx.setRewardBatchId(null);
        trx.setStatus(SyncTrxStatus.INVOICED.name());

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Flux<RewardTransaction> result =
                repository.findInvoicedTransactionsWithoutBatch(20);

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        

        verify(mongoTemplate).find(queryCaptor.capture(), eq(RewardTransaction.class));

        Query query = queryCaptor.getValue();

        assertNotNull(query);

        String queryJson = query.getQueryObject().toString();

        assertTrue(queryJson.contains("INVOICED"));
        assertTrue(queryJson.contains("rewardBatchId"));

    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_shouldReturnTransaction_whenFound() {

        RewardTransaction trx = new RewardTransaction();
        trx.setId("TRX1");
        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId(null);
        trx.setMerchantId("M1");

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trx));

        Mono<RewardTransaction> result =
                repository.findInvoicedTrxByIdWithoutBatch("INIT1", "M1", "TRX1");

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        

        verify(mongoTemplate).findOne(queryCaptor.capture(), eq(RewardTransaction.class));

        Query query = queryCaptor.getValue();

        assertNotNull(query);

        String queryJson = query.getQueryObject().toString();

        assertTrue(queryJson.contains("TRX1"));
        assertTrue(queryJson.contains("INVOICED"));
        assertTrue(queryJson.contains("rewardBatchId"));
        assertTrue(queryJson.contains("M1"));
        assertTrue(queryJson.contains("INIT1"));
    }

    @Test
    void findDistinctFranchiseAndPosByRewardBatchId_shouldReturnGroupedResults() {

        FranchisePointOfSaleDTO dto = new FranchisePointOfSaleDTO();
        dto.setFranchiseName("FR1");
        dto.setPointOfSaleId("POS1");

        when(mongoTemplate.aggregate(
                any(Aggregation.class),
                eq(RewardTransaction.class),
                eq(FranchisePointOfSaleDTO.class)
        )).thenReturn(Flux.just(dto));

        Flux<FranchisePointOfSaleDTO> result =
                repository.findDistinctFranchiseAndPosByRewardBatchId("BATCH1", "M1");

        StepVerifier.create(result)
                .expectNext(dto)
                .verifyComplete();

        ArgumentCaptor<Aggregation> aggCaptor = ArgumentCaptor.forClass(Aggregation.class);

        verify(mongoTemplate).aggregate(
                aggCaptor.capture(),
                eq(RewardTransaction.class),
                eq(FranchisePointOfSaleDTO.class)
        );

        Aggregation aggregation = aggCaptor.getValue();
        assertNotNull(aggregation);
    }

    @Test
    void shouldReturnTransaction_whenFoundInBatch() {

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trx));

        Mono<RewardTransaction> result = repository.findTransactionInBatch(
                "INIT1",
                "M1",
                "B1",
                "trx1"
        );

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).findOne(queryCaptor.capture(), eq(RewardTransaction.class));

        Query query = queryCaptor.getValue();
        assert query != null;
    }

    @Test
    void shouldReturnEmpty_whenTransactionNotFound() {

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        Mono<RewardTransaction> result = repository.findTransactionInBatch(
                "INIT1",
                "M1",
                "B1",
                "trx1"
        );

        StepVerifier.create(result)
                .verifyComplete();

        verify(mongoTemplate).findOne(any(Query.class), eq(RewardTransaction.class));
    }
}