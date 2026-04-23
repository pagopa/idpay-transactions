package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@MongoTest
class RewardTransactionSpecificRepositoryTest {

    @Autowired
    private RewardTransactionSpecificRepositoryImpl repository;

    @Autowired
    private RewardTransactionRepository baseRepository;

    private RewardTransaction trx;

    @BeforeEach
    void setup() {
        baseRepository.deleteAll().block();

        trx = RewardTransaction.builder()
                .id("trx1")
                .merchantId("M1")
                .userId("U1")
                .initiatives(List.of("INIT_1"))
                .trxDate(LocalDateTime.now())
                .amountCents(100L)
                .status(SyncTrxStatus.REWARDED.name())
                .rewardBatchId("B1")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .build();

        baseRepository.save(trx).block();
    }


    @Test
    void findByIdTrxIssuer_onlyIssuer_shouldReturnAllMatching() {
        trx.setIdTrxIssuer("ISSUER1");
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByIdTrxIssuer("ISSUER1", null, null, null, null, PageRequest.of(0, 10))
                )
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findByIdTrxIssuer_withUserId_shouldFilter() {
        trx.setIdTrxIssuer("ISSUER1");
        trx.setUserId("USER1");
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByIdTrxIssuer("ISSUER1", "USER1", null, null, null, PageRequest.of(0, 10))
                )
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(
                        repository.findByIdTrxIssuer("ISSUER1", "WRONG", null, null, null, PageRequest.of(0, 10))
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findByIdTrxIssuer_withAmount_shouldFilter() {
        trx.setIdTrxIssuer("ISSUER1");
        trx.setAmountCents(100L);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByIdTrxIssuer("ISSUER1", null, null, null, 100L, PageRequest.of(0, 10))
                )
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(
                        repository.findByIdTrxIssuer("ISSUER1", null, null, null, 999L, PageRequest.of(0, 10))
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findByIdTrxIssuer_withDateRange_shouldFilterBetween() {
        LocalDateTime now = LocalDateTime.of(2025, 1, 1, 10, 0);

        trx.setIdTrxIssuer("ISSUER1");
        trx.setTrxDate(now);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISSUER1",
                                null,
                                now.minusHours(1),
                                now.plusHours(1),
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISSUER1",
                                null,
                                now.plusHours(1),
                                now.plusHours(2),
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findByIdTrxIssuer_onlyStartDate_shouldFilterGte() {
        LocalDateTime now = LocalDateTime.of(2025, 1, 1, 10, 0);

        trx.setIdTrxIssuer("ISSUER1");
        trx.setTrxDate(now);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISSUER1",
                                null,
                                now.minusHours(1),
                                null,
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISSUER1",
                                null,
                                now.plusHours(1),
                                null,
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findByIdTrxIssuer_onlyEndDate_shouldFilterLte() {
        LocalDateTime now = LocalDateTime.of(2025, 1, 1, 10, 0);

        trx.setIdTrxIssuer("ISSUER1");
        trx.setTrxDate(now);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISSUER1",
                                null,
                                null,
                                now.plusHours(1),
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISSUER1",
                                null,
                                null,
                                now.minusHours(1),
                                null,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findByIdTrxIssuer_allFiltersCombined_shouldWorkTogether() {
        LocalDateTime now = LocalDateTime.of(2025, 1, 1, 10, 0);

        trx.setIdTrxIssuer("ISSUER1");
        trx.setUserId("USER1");
        trx.setAmountCents(100L);
        trx.setTrxDate(now);

        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByIdTrxIssuer(
                                "ISSUER1",
                                "USER1",
                                now.minusHours(1),
                                now.plusHours(1),
                                100L,
                                PageRequest.of(0, 10)
                        )
                )
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findByRange_shouldReturnResults() {
        StepVerifier.create(
                        repository.findByRange("U1",
                                LocalDateTime.now().minusDays(1),
                                LocalDateTime.now().plusDays(1),
                                100L,
                                PageRequest.of(0, 10))
                )
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findByFilter_shouldApplyFilters() {
        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");

        StepVerifier.create(
                        repository.findByFilter(filters, "U1", false, PageRequest.of(0, 10))
                )
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findTransaction_shouldReturnOnlyValidStatuses() {
        StepVerifier.create(repository.findTransaction("M1", "trx1"))
                .expectNextMatches(t -> t.getId().equals("trx1"))
                .verifyComplete();
    }

    @Test
    void updateStatusAndReturnOld_shouldUpdateAndReturnOld() {

        StepVerifier.create(
                        repository.updateStatusAndReturnOld(
                                "INIT_1",
                                "B1",
                                "trx1",
                                RewardBatchTrxStatus.SUSPENDED,
                                null,
                                "2025-01",
                                null
                        )
                )
                .assertNext(old -> assertEquals(RewardBatchTrxStatus.CONSULTABLE, old.getRewardBatchTrxStatus()))
                .verifyComplete();

        RewardTransaction updated = baseRepository.findById("trx1").block();
        assertNotNull(updated);
        assertEquals(RewardBatchTrxStatus.SUSPENDED, updated.getRewardBatchTrxStatus());
    }

    @Test
    void updateStatusAndReturnOld_withChecksError_shouldSetChecksError() {

        StepVerifier.create(
                        repository.updateStatusAndReturnOld(
                                "INIT_1",
                                "B1",
                                "trx1",
                                RewardBatchTrxStatus.SUSPENDED,
                                null,
                                "2025-01",
                                new it.gov.pagopa.idpay.transactions.model.ChecksError()
                        )
                )
                .expectNextCount(1)
                .verifyComplete();

        RewardTransaction updated = baseRepository.findById("trx1").block();
        assertNotNull(updated);
        assertNotNull(updated.getChecksError());
    }

    @Test
    void removeInitiative_shouldRemoveData() {

        StepVerifier.create(
                repository.removeInitiativeOnTransaction("trx1", "INIT_1")
        ).verifyComplete();

        RewardTransaction updated = baseRepository.findById("trx1").block();
        assertNotNull(updated);
        assertFalse(updated.getInitiatives().contains("INIT_1"));
    }

    @Test
    void rewardTransactions_shouldUpdateStatus() {

        StepVerifier.create(
                repository.rewardTransactionsByBatchIdAndInitiativeId("B1", "INIT_1")
        ).verifyComplete();

        RewardTransaction updated = baseRepository.findById("trx1").block();
        assertNotNull(updated);
        assertEquals(SyncTrxStatus.REWARDED.name(), updated.getStatus());
    }

    @Test
    void sumSuspended_shouldReturnZeroWhenEmpty() {

        StepVerifier.create(
                        repository.sumSuspendedAccruedRewardCents("INIT_1", "B1")
                )
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void findInvoicedTransactionsWithoutBatch_shouldReturnOnlyMatching() {

        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId(null);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findInvoicedTransactionsWithoutBatch(10)
                )
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findTransactionInBatch_shouldReturnMatch() {
        StepVerifier.create(
                        repository.findTransactionInBatch("INIT_1", "M1", "B1", "trx1")
                )
                .expectNextMatches(t -> t.getId().equals("trx1"))
                .verifyComplete();
    }

    @Test
    void findByInitiativeIdAndUserId_shouldWork() {
        StepVerifier.create(
                        repository.findByInitiativeIdAndUserId("INIT_1", "U1")
                )
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void getCount_withoutStatus_shouldUseDefaultStatuses() {
        trx.setStatus(SyncTrxStatus.REWARDED.name());
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setStatus(null);

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_withExplicitStatus_shouldFilter() {
        trx.setStatus(SyncTrxStatus.REWARDED.name());
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setStatus(SyncTrxStatus.REWARDED.name());

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();

        filters.setStatus("CANCELLED");

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void getCount_withTrxCode_shouldApplyRegex() {
        trx.setTrxCode("CODE123");
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setTrxCode("CODE");

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();

        filters.setTrxCode("NO_MATCH");

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void getCount_withRewardBatchId_shouldFilter() {
        trx.setRewardBatchId("B1");
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setRewardBatchId("B1");

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();

        filters.setRewardBatchId("WRONG");

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void getCount_withRewardBatchTrxStatus_shouldFilter() {
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_consultableWithIncludeToCheck_shouldIncludeToCheck() {

        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        baseRepository.save(trx).block();

        RewardTransaction trx2 = RewardTransaction.builder()
                .id("trx2")
                .merchantId("M1")
                .userId("U1")
                .initiatives(List.of("INIT_1"))
                .trxDate(LocalDateTime.now())
                .amountCents(100L)
                .status(SyncTrxStatus.REWARDED.name())
                .rewardBatchId("B1")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .build();

        baseRepository.save(trx2).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, true)
                )
                .expectNext(2L)
                .verifyComplete();
    }

    @Test
    void getCount_consultableWithoutIncludeToCheck_shouldNotIncludeToCheck() {

        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        baseRepository.save(trx).block();

        RewardTransaction trx2 = RewardTransaction.builder()
                .id("trx2")
                .merchantId("M1")
                .userId("U1")
                .initiatives(List.of("INIT_1"))
                .trxDate(LocalDateTime.now())
                .amountCents(100L)
                .status(SyncTrxStatus.REWARDED.name())
                .rewardBatchId("B1")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .build();

        baseRepository.save(trx2).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        StepVerifier.create(
                        repository.getCount(filters, null, null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void getCount_withPointOfSaleId_shouldFilter() {
        trx.setPointOfSaleId("POS1");
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");

        StepVerifier.create(
                        repository.getCount(filters, "POS1", null, null, false)
                )
                .expectNext(1L)
                .verifyComplete();

        StepVerifier.create(
                        repository.getCount(filters, "WRONG", "productGtin", null, false)
                )
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void findByFilterTrx_shouldUseAggregation_whenSortingByStatus() {

        trx.setStatus(SyncTrxStatus.REWARDED.name());
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");

        Pageable pageable = PageRequest.of(
                0,
                10,
                Sort.by(Sort.Direction.ASC, "status")
        );

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
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findByFilterTrx_shouldUseFind_whenNotSortingByStatus() {

        trx.setStatus(SyncTrxStatus.REWARDED.name());
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");

        Pageable pageable = PageRequest.of(
                0,
                10,
                Sort.by(Sort.Direction.DESC, "trxDate")
        );

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
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findByFilterTrx_shouldMapProductNameSortField() {
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");

        Pageable pageable = PageRequest.of(
                0,
                10,
                Sort.by(Sort.Direction.ASC, "productName")
        );

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
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findByFilterTrx_shouldHandleNullPageable() {

        trx.setStatus(SyncTrxStatus.REWARDED.name());
        baseRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");

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
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findByFilterTrx_consultable_shouldIncludeToCheck() {

        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        baseRepository.save(trx).block();

        RewardTransaction trx2 = RewardTransaction.builder()
                .id("trx2")
                .merchantId("M1")
                .userId("U1")
                .initiatives(List.of("INIT_1"))
                .trxDate(LocalDateTime.now())
                .amountCents(100L)
                .status(SyncTrxStatus.REWARDED.name())
                .rewardBatchId("B1")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .build();

        baseRepository.save(trx2).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        Pageable pageable = PageRequest.of(0, 10);

        StepVerifier.create(
                        repository.findByFilterTrx(
                                filters,
                                null,
                                "U1",
                                null,
                                true,
                                pageable
                        )
                )
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void findByFilterTrx_consultable_shouldIncludeToCheckPageableNull() {

        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        baseRepository.save(trx).block();

        RewardTransaction trx2 = RewardTransaction.builder()
                .id("trx2")
                .merchantId("M1")
                .userId("U1")
                .initiatives(List.of("INIT_1"))
                .trxDate(LocalDateTime.now())
                .amountCents(100L)
                .status(SyncTrxStatus.REWARDED.name())
                .rewardBatchId("B1")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .build();

        baseRepository.save(trx2).block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId("M1");
        filters.setInitiativeId("INIT_1");
        filters.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);

        StepVerifier.create(
                        repository.findByFilterTrx(
                                filters,
                                null,
                                "U1",
                                null,
                                true,
                                null
                        )
                )
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void findByFilter_shouldReturnMatchingTransaction() {
        StepVerifier.create(
                        repository.findByFilter(
                                "B1",
                                "INIT_1",
                                List.of(RewardBatchTrxStatus.CONSULTABLE)
                        )
                )
                .expectNextMatches(t ->
                        t.getId().equals("trx1") &&
                                t.getRewardBatchId().equals("B1") &&
                                t.getInitiatives().contains("INIT_1")
                )
                .verifyComplete();
    }

    @Test
    void findByFilter_shouldReturnEmpty_whenBatchIdDoesNotMatch() {
        StepVerifier.create(
                        repository.findByFilter(
                                "WRONG_BATCH",
                                "INIT_1",
                                List.of(RewardBatchTrxStatus.CONSULTABLE)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findByFilter_shouldReturnEmpty_whenInitiativeDoesNotMatch() {
        StepVerifier.create(
                        repository.findByFilter(
                                "B1",
                                "WRONG_INIT",
                                List.of(RewardBatchTrxStatus.CONSULTABLE)
                        )
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findByFilter_shouldFilterByStatusList() {
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findByFilter(
                                "B1",
                                "INIT_1",
                                List.of(RewardBatchTrxStatus.CONSULTABLE)
                        )
                )
                .expectNextCount(1)
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
    }

    @Test
    void findByFilter_shouldWorkWithMultipleStatuses() {
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        baseRepository.save(trx).block();

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
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void findOneByInitiativeId_shouldReturnTransaction() {
        StepVerifier.create(
                        repository.findOneByInitiativeId("INIT_1")
                )
                .expectNextMatches(t ->
                        t.getId().equals("trx1") &&
                                t.getInitiatives().contains("INIT_1")
                )
                .verifyComplete();
    }

    @Test
    void findOneByInitiativeId_shouldReturnEmpty_whenNotFound() {
        StepVerifier.create(
                        repository.findOneByInitiativeId("WRONG_INIT")
                )
                .verifyComplete();
    }

    @Test
    void findOneByInitiativeId_shouldMatchSingleElementInsideList() {
        trx.setInitiatives(List.of("INIT_1", "INIT_2"));
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findOneByInitiativeId("INIT_2")
                )
                .expectNextMatches(t ->
                        t.getInitiatives().contains("INIT_2")
                )
                .verifyComplete();
    }

    @Test
    void findByInitiativesWithBatch_shouldReturnMatchingTransactions() {
        StepVerifier.create(
                        repository.findByInitiativesWithBatch("INIT_1", 10)
                )
                .expectNextMatches(t ->
                        t.getId().equals("trx1") &&
                                t.getInitiatives().contains("INIT_1")
                )
                .verifyComplete();
    }

    @Test
    void findByInitiativesWithBatch_shouldReturnEmpty_whenInitiativeNotFound() {
        StepVerifier.create(
                        repository.findByInitiativesWithBatch("WRONG_INIT", 10)
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void rewardTransactionsByBatchIdAndInitiativeId_shouldUpdateAndSample() {

        StepVerifier.create(
                        repository.rewardTransactionsByBatchIdAndInitiativeId("B1", "INIT_1")
                )
                .verifyComplete();

        RewardTransaction updated = baseRepository.findById("trx1").block();

        assertNotNull(updated);
        assertEquals(SyncTrxStatus.REWARDED.name(), updated.getStatus());
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_shouldReturnTransaction() {

        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId(null);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findInvoicedTrxByIdWithoutBatch("INIT_1", "M1", "trx1")
                )
                .expectNextMatches(t ->
                        t.getId().equals("trx1") &&
                                SyncTrxStatus.INVOICED.name().equals(t.getStatus())
                )
                .verifyComplete();
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_shouldReturnEmpty_whenBatchExists() {

        trx.setStatus(SyncTrxStatus.INVOICED.name());
        trx.setRewardBatchId("B1"); // NON null
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findInvoicedTrxByIdWithoutBatch("INIT_1", "M1", "trx1")
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_shouldReturnEmpty_whenStatusNotInvoiced() {

        trx.setStatus(SyncTrxStatus.REWARDED.name());
        trx.setRewardBatchId(null);
        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findInvoicedTrxByIdWithoutBatch("INIT_1", "M1", "trx1")
                )
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    void findDistinctFranchiseAndPosByRewardBatchId_shouldReturnAggregatedResult() {

        trx.setRewardBatchId("B1");
        trx.setMerchantId("M1");
        trx.setFranchiseName("FR1");
        trx.setPointOfSaleId("POS1");

        baseRepository.save(trx).block();

        StepVerifier.create(
                        repository.findDistinctFranchiseAndPosByRewardBatchId("B1", "M1")
                )
                .expectNextMatches(dto ->
                        "FR1".equals(dto.getFranchiseName()) &&
                                "POS1".equals(dto.getPointOfSaleId())
                )
                .verifyComplete();
    }

    @Test
    void findDistinctFranchiseAndPosByRewardBatchId_shouldReturnEmpty_whenNoMatch() {

        StepVerifier.create(
                        repository.findDistinctFranchiseAndPosByRewardBatchId("WRONG", "M1")
                )
                .expectNextCount(0)
                .verifyComplete();
    }

}