package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.*;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardTransactionSpecificRepositoryTest {

    @Mock
    private ReactiveMongoTemplate template;

    @InjectMocks
    private RewardTransactionSpecificRepositoryImpl repo;


    @Test
    void testFindByIdTrxIssuer_fullFilters() {
        RewardTransaction trx = new RewardTransaction();
        when(template.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(repo.findByIdTrxIssuer(
                        "trxIssuer123",
                        "userABC",
                        LocalDateTime.MIN,
                        LocalDateTime.MAX,
                        100L,
                        pageable))
                .expectNext(trx)
                .verifyComplete();

        verify(template).find(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void testFindByRange_basic() {
        RewardTransaction trx = new RewardTransaction();
        when(template.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(repo.findByRange(
                        "user123",
                        LocalDateTime.MIN,
                        LocalDateTime.MAX,
                        200L,
                        PageRequest.of(0, 5)))
                .expectNext(trx)
                .verifyComplete();

        verify(template).find(any(Query.class), eq(RewardTransaction.class));
    }


    @Test
    void testFindOneByInitiativeId() {
        RewardTransaction trx = new RewardTransaction();
        when(template.findOne(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(trx));

        StepVerifier.create(repo.findOneByInitiativeId("INIT_01"))
                .expectNext(trx)
                .verifyComplete();

        verify(template).findOne(any(Query.class), eq(RewardTransaction.class));
    }

    @Test
    void testGetCount() {
        when(template.count(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.just(10L));

        StepVerifier.create(repo.getCount("M1", "I1", null, null, "U1", "REWARDED"))
                .expectNext(10L)
                .verifyComplete();
    }


    @Test
    void testRemoveInitiativeOnTransaction() {
        when(template.updateFirst(any(Query.class), any(Update.class), eq(RewardTransaction.class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(repo.removeInitiativeOnTransaction("T123", "INIT99"))
                .verifyComplete();

        verify(template).updateFirst(any(Query.class), any(Update.class), eq(RewardTransaction.class));
    }

    @Test
    void testFindByInitiativesWithBatch() {
        RewardTransaction trx = new RewardTransaction();
        when(template.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        StepVerifier.create(repo.findByInitiativesWithBatch("INIT01", 100))
                .expectNext(trx)
                .verifyComplete();

        verify(template).find(any(Query.class), eq(RewardTransaction.class));
    }


    @Test
    void testFindByFilter() {
        RewardTransaction trx = new RewardTransaction();

        when(template.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        TrxFiltersDTO filters = new TrxFiltersDTO(
                "MERCHANT", "INITIATIVE",
                null, // productGtin
                null, // status → triggers default ["CANCELLED","REWARDED","REFUNDED","INVOICED"]
                null,
                null);

        StepVerifier.create(repo.findByFilter(filters, "USER1", PageRequest.of(0, 10)))
                .expectNext(trx)
                .verifyComplete();

        verify(template).find(any(Query.class), eq(RewardTransaction.class));
    }


    @Test
    void testFindByFilterTrx_aggregationBranch() {
        RewardTransaction trx = new RewardTransaction();
        when(template.aggregate(any(Aggregation.class), eq(RewardTransaction.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Pageable pageable = PageRequest.of(0, 10,
                Sort.by(Sort.Order.asc("status")));

        StepVerifier.create(repo.findByFilterTrx(
                        "MERCHANT",
                        "INITIATIVE",
                        "POS1",
                        "USER1",
                        "GTIN",
                        "REWARDED",
                        pageable))
                .expectNext(trx)
                .verifyComplete();

        verify(template).aggregate(any(Aggregation.class), eq(RewardTransaction.class), eq(RewardTransaction.class));
    }

    @Test
    void testFindByFilterTrx_fallbackFind() {
        RewardTransaction trx = new RewardTransaction();
        when(template.find(any(Query.class), eq(RewardTransaction.class)))
                .thenReturn(Flux.just(trx));

        Pageable pageable = PageRequest.of(0, 10, Sort.by("otherField"));

        StepVerifier.create(repo.findByFilterTrx(
                        "MERCHANT",
                        "INITIATIVE",
                        null,
                        "USERX",
                        null,
                        "CANCELLED",
                        pageable))
                .expectNext(trx)
                .verifyComplete();

        verify(template).find(any(Query.class), eq(RewardTransaction.class));
    }
}
