package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RewardTransactionSpecificRepositoryTest {

    @Mock
    private ReactiveMongoTemplate mongoTemplate;

    @InjectMocks
    private RewardTransactionSpecificRepositoryImpl repository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void findByIdTrxIssuer_shouldReturnTransactions() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Flux<RewardTransaction> result = repository.findByIdTrxIssuer(
                "trxIssuer",
                "user1",
                LocalDateTime.now().minusDays(1),
                LocalDateTime.now(),
                100L,
                PageRequest.of(0, 10)
        );

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void findByIdTrxIssuer_shouldReturnTransactions2() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Flux<RewardTransaction> result = repository.findByIdTrxIssuer(
                "trxIssuer",
                "user1",
                null,
                LocalDateTime.now(),
                100L,
                PageRequest.of(0, 10)
        );

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void findByFilterTrx_sreturnTransaction() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        TrxFiltersDTO trxFiltersDTO = new TrxFiltersDTO();
        trxFiltersDTO.setPointOfSaleId("pointOfSaleId");
        trxFiltersDTO.setTrxCode("trxCode");
        trxFiltersDTO.setInitiativeId("initiativeId");
        trxFiltersDTO.setStatus("true");
        trxFiltersDTO.setMerchantId("merchantId");
        trxFiltersDTO.setFiscalCode("fiscalCode");
        trxFiltersDTO.setRewardBatchId("rewardBatch");
        trxFiltersDTO.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        Pageable pageable = PageRequest.of(5,5);
        Flux<RewardTransaction> result = repository.findByFilterTrx(trxFiltersDTO,"pointOfSaleID","userId","productGtin",true,pageable);

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void findByFilterTrx_sreturnTransaction2() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        when(mongoTemplate.aggregate(
                any(Aggregation.class),
                eq(RewardTransaction.class),
                eq(RewardTransaction.class)
        )).thenReturn(Flux.just(trx));

        TrxFiltersDTO trxFiltersDTO = new TrxFiltersDTO();
        trxFiltersDTO.setPointOfSaleId("pointOfSaleId");
        trxFiltersDTO.setTrxCode("trxCode");
        trxFiltersDTO.setInitiativeId("initiativeId");
        trxFiltersDTO.setStatus("true");
        trxFiltersDTO.setMerchantId("merchantId");
        trxFiltersDTO.setFiscalCode("fiscalCode");
        trxFiltersDTO.setRewardBatchId("rewardBatch");
        trxFiltersDTO.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
        Pageable pageable = PageRequest.of(
                0,
                10,
                Sort.by(Sort.Order.asc("status"))
        );
        Flux<RewardTransaction> result = repository.findByFilterTrx(trxFiltersDTO,"pointOfSaleID","userId","productGtin",true,pageable);

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).aggregate(
                any(Aggregation.class),
                eq(RewardTransaction.class),
                eq(RewardTransaction.class)
        );
    }

    @Test
    void findByIdTrxIssuer_shouldReturnTransactions3() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Flux<RewardTransaction> result = repository.findByIdTrxIssuer(
                "trxIssuer",
                "user1",
                LocalDateTime.now().minusDays(1),
                null,
                100L,
                PageRequest.of(0, 10)
        );

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();

        verify(mongoTemplate).find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void findByRange_shouldReturnTransactions() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Flux<RewardTransaction> result = repository.findByRange(
                "user1",
                LocalDateTime.now().minusDays(5),
                LocalDateTime.now(),
                100L,
                PageRequest.of(0, 10)
        );

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void findTransaction_shouldReturnTransaction() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trx));

        Mono<RewardTransaction> result = repository.findTransaction("merchant1", "trx1");

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void findTransaction_shouldReturnEmpty_whenNotFound() {
        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(repository.findTransaction("merchant1", "trx1"))
                .verifyComplete();
    }

    @Test
    void findByFilter_shouldReturnTransactions() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("merchant1");
        filters.setInitiativeId("init1");

        Flux<RewardTransaction> result = repository.findByFilter(
                filters,
                "user1",
                false,
                PageRequest.of(0, 10)
        );

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void findByFilter_shouldReturnTransactions2() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setPointOfSaleId("pointOfSale1");
        filters.setMerchantId("merchant1");
        filters.setInitiativeId("init1");

        Flux<RewardTransaction> result = repository.findByFilter(
                filters,
                "user1",
                false,
                PageRequest.of(0, 10)
        );

        StepVerifier.create(result)
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void getCount_shouldReturnCount() {
        when(mongoTemplate.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(5L));

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("merchant1");
        filters.setInitiativeId("init1");

        StepVerifier.create(repository.getCount(filters, "pointOfSaleId", "productGtin", "userId", false))
                .expectNext(5L)
                .verifyComplete();
    }

    @Test
    void findInvoicedTransactionsWithoutBatch_shouldReturnTransactions() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(repository.findInvoicedTransactionsWithoutBatch(10))
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_shouldReturnTransaction() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(repository.findInvoicedTrxByIdWithoutBatch("trx1"))
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void rewardTransactionsByBatchId_shouldExecuteFlow() {
        when(mongoTemplate.updateMulti(any(Query.class), any(), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(mock(com.mongodb.client.result.UpdateResult.class)));

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(new RewardTransaction()));

        StepVerifier.create(repository.rewardTransactionsByBatchId("batch1"))
                .verifyComplete();
    }

    @Test
    void findByInitiativeIdAndUserId_shouldReturnTransactions() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(repository.findByInitiativeIdAndUserId("init1", "user1"))
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void findTransactionInBatch_shouldReturnTransaction() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(repository.findTransactionInBatch("merchant1", "batch1", "trx1"))
                .expectNext(trx)
                .verifyComplete();
    }

    @Test
    void findByFilter_withStatusList_shouldReturnTransactions() {
        RewardTransaction trx = new RewardTransaction();

        when(mongoTemplate.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(repository.findByFilter(
                        "batch1",
                        "init1",
                        List.of(RewardBatchTrxStatus.CONSULTABLE)))
                .expectNext(trx)
                .verifyComplete();
    }
}