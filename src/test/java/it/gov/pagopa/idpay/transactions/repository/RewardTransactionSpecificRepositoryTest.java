package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.test.fakers.RewardTransactionFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
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

    private static final String INITIATIVE_ID = "INITIATIVEID1";
    private static final String MERCHANT_ID = "MERCHANTID1";
    private static final String USER_ID = "USERID1";
    private static final String POS_ID = "POINTOFSALEID1";
    private static final String BATCH_ID = "BATCH_TEST";
    private static final String PRODUCT_GTIN = "GTIN-ABC-123";

    @BeforeEach
    void cleanBefore() {
        rewardTransactionRepository.deleteAll().block();
    }

    @AfterEach
    void cleanAfter() {
        rewardTransactionRepository.deleteAll().block();
    }

    @Test
    void findByFilterTrx_withNullPageable_shouldUseDefaultSortAndNotFail() {
        RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .trxChargeDate(LocalDateTime.now())
                .additionalProperties(Map.of("productName", "AAA"))
                .build();
        rewardTransactionRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                SyncTrxStatus.REWARDED.name(),
                null,
                null,
                null,
                null
        );

        List<RewardTransaction> out = rewardTransactionSpecificRepository
                .findByFilterTrx(filters, POS_ID, USER_ID, null, false, null)
                .toStream()
                .toList();

        assertEquals(1, out.size());
        assertEquals("t1", out.getFirst().getId());
    }

    @Test
    void findByFilter_withNullPageable_shouldNotFail() {
        RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();
        rewardTransactionRepository.save(trx).block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                SyncTrxStatus.INVOICED.name(),
                null,
                null,
                null,
                null
        );

        List<RewardTransaction> out = rewardTransactionSpecificRepository
                .findByFilter(filters, USER_ID, false, null)
                .toStream()
                .toList();

        assertEquals(1, out.size());
        assertEquals("t1", out.getFirst().getId());
    }

    @Test
    void findByFilterTrx_whenFiltersStatusBlank_shouldApplyDefaultStatuses() {
        RewardTransaction invoiced = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("inv")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction cancelled = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("can")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.CANCELLED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction other = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("oth")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status("CREATED")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(invoiced, cancelled, other)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                null,
                null,
                null,
                null,
                null
        );

        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "_id"));

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilterTrx(filters, POS_ID, USER_ID, "", false, pageable)
                .map(RewardTransaction::getId)
                .toStream()
                .toList();

        assertTrue(ids.contains("inv"));
        assertTrue(ids.contains("can"));
        assertFalse(ids.contains("oth"));
    }


    @Test
    void findByFilter_withRewardBatchId_andRewardBatchTrxStatusNotConsultable_shouldFilterExact() {
        RewardTransaction approved = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("app")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchId(BATCH_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction toCheckSameBatch = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("toc")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchId(BATCH_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(approved, toCheckSameBatch)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                SyncTrxStatus.INVOICED.name(),
                BATCH_ID,
                RewardBatchTrxStatus.APPROVED,
                null,
                null
        );

        List<RewardTransaction> res = rewardTransactionSpecificRepository
                .findByFilter(filters, USER_ID, true, PageRequest.of(0, 10))
                .toStream()
                .toList();

        assertEquals(1, res.size());
        assertEquals("app", res.getFirst().getId());
    }

    @Test
    void findByFilterTrx_withProductGtinRegex_shouldMatchCaseInsensitiveSubstring() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .additionalProperties(Map.of("productGtin", "xxx-" + PRODUCT_GTIN.toLowerCase() + "-yyy"))
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("t2")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .additionalProperties(Map.of("productGtin", "NO_MATCH"))
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                SyncTrxStatus.REWARDED.name(),
                null,
                null,
                null,
                null
        );

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilterTrx(filters, POS_ID, USER_ID, PRODUCT_GTIN.toUpperCase(), false,
                        PageRequest.of(0, 10))
                .map(RewardTransaction::getId)
                .toStream()
                .toList();

        assertEquals(List.of("t1"), ids);
    }

    @Test
    void findByFilterTrx_sortByProductName_shouldNotCrashAndReturnData() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .additionalProperties(Map.of("productName", "B"))
                .trxChargeDate(LocalDateTime.now())
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("t2")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .additionalProperties(Map.of("productName", "A"))
                .trxChargeDate(LocalDateTime.now().minusMinutes(1))
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                SyncTrxStatus.REWARDED.name(),
                null,
                null,
                null,
                null
        );

        Pageable sortByProductName = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "productName"));

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilterTrx(filters, POS_ID, USER_ID, "", false, sortByProductName)
                .map(RewardTransaction::getId)
                .toStream()
                .toList();

        assertEquals(2, ids.size());
        assertTrue(ids.contains("t1"));
        assertTrue(ids.contains("t2"));
    }

    @Test
    void findByFilterTrx_statusAggregation_shouldRankStatuses() {
        RewardTransaction c = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("c")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.CANCELLED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction i = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("i")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction r = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("r")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction rf = RewardTransactionFaker.mockInstanceBuilder(4)
                .id("rf")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REFUNDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(rf, r, i, c)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                null,
                null,
                null,
                null,
                null
        );

        Pageable sortByStatus = PageRequest.of(0, 10, Sort.by("status"));

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilterTrx(filters, POS_ID, USER_ID, "", false, sortByStatus)
                .map(RewardTransaction::getId)
                .toStream()
                .toList();

        assertEquals(List.of("c", "i", "r", "rf"), ids);
    }

    @Test
    void rewardTransactionsByBatchId_whenNoTransactions_shouldComplete() {
        StepVerifier.create(rewardTransactionSpecificRepository.rewardTransactionsByBatchId("BATCH_NONE"))
                .verifyComplete();
    }

    @Test
    void rewardTransactionsByBatchId_whenOnlySuspended_shouldUpdateStatusButNoSamplingUpdate() {
        String batchId = "BATCH_SUSP_ONLY";

        RewardTransaction s1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("s1")
                .rewardBatchId(batchId)
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .samplingKey(1)
                .build();

        RewardTransaction s2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("s2")
                .rewardBatchId(batchId)
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .samplingKey(2)
                .build();

        rewardTransactionRepository.saveAll(List.of(s1, s2)).collectList().block();

        rewardTransactionSpecificRepository.rewardTransactionsByBatchId(batchId).block();

        RewardTransaction after1 = rewardTransactionRepository.findById("s1").block();
        RewardTransaction after2 = rewardTransactionRepository.findById("s2").block();

        assertNotNull(after1);
        assertNotNull(after2);
        assertEquals(SyncTrxStatus.REWARDED.name(), after1.getStatus());
        assertEquals(SyncTrxStatus.REWARDED.name(), after2.getStatus());
        assertEquals(RewardBatchTrxStatus.SUSPENDED, after1.getRewardBatchTrxStatus());
        assertEquals(RewardBatchTrxStatus.SUSPENDED, after2.getRewardBatchTrxStatus());
    }

    @Test
    void sumSuspendedAccruedRewardCents_whenNoMatches_shouldReturnZero() {
        Long sum = rewardTransactionSpecificRepository.sumSuspendedAccruedRewardCents("NO_BATCH").block();
        assertNotNull(sum);
        assertEquals(0L, sum);
    }

    @Test
    void findTransactionForUpdateInvoice_shouldReturnTransactionRegardlessStatus() {
        RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .merchantId(MERCHANT_ID)
                .pointOfSaleId(POS_ID)
                .status("CREATED")
                .build();
        rewardTransactionRepository.save(trx).block();

        RewardTransaction found = rewardTransactionSpecificRepository
                .findTransactionForUpdateInvoice(MERCHANT_ID, POS_ID, "t1")
                .block();

        assertNotNull(found);
        assertEquals("t1", found.getId());
    }

    @Test
    void findInvoicedTransactionsWithoutBatch_shouldRespectPageSize() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1").status(SyncTrxStatus.INVOICED.name()).rewardBatchId(null).build();
        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("t2").status(SyncTrxStatus.INVOICED.name()).rewardBatchId(null).build();
        RewardTransaction t3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("t3").status(SyncTrxStatus.INVOICED.name()).rewardBatchId(null).build();

        rewardTransactionRepository.saveAll(List.of(t1, t2, t3)).collectList().block();

        List<RewardTransaction> page2 = rewardTransactionSpecificRepository
                .findInvoicedTransactionsWithoutBatch(2)
                .collectList()
                .block();

        assertNotNull(page2);
        assertEquals(2, page2.size());
    }


    @Test
    void findTransactionInBatch_shouldReturnMatching() {
        RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .merchantId(MERCHANT_ID)
                .rewardBatchId(BATCH_ID)
                .build();
        rewardTransactionRepository.save(trx).block();

        RewardTransaction found = rewardTransactionSpecificRepository
                .findTransactionInBatch(MERCHANT_ID, BATCH_ID, "t1")
                .block();

        assertNotNull(found);
        assertEquals("t1", found.getId());
    }

    @Test
    void removeInitiativeOnTransaction_shouldAlsoUnsetRewardsAndRejectionReasons() {
        RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .initiatives(List.of(INITIATIVE_ID, "OTHER"))
                .rewards(Map.of(
                        INITIATIVE_ID, Reward.builder().accruedRewardCents(100L).build(),
                        "OTHER", Reward.builder().accruedRewardCents(200L).build()
                ))
                .initiativeRejectionReasons(Map.of(
                        INITIATIVE_ID, List.of("R1"),
                        "OTHER", List.of("R2")
                ))
                .build();

        rewardTransactionRepository.save(trx).block();

        rewardTransactionSpecificRepository.removeInitiativeOnTransaction("t1", INITIATIVE_ID).block();

        RewardTransaction after = rewardTransactionRepository.findById("t1").block();
        assertNotNull(after);

        assertFalse(after.getInitiatives().contains(INITIATIVE_ID));
        assertTrue(after.getInitiatives().contains("OTHER"));

        assertNotNull(after.getRewards());
        assertFalse(after.getRewards().containsKey(INITIATIVE_ID));
        assertTrue(after.getRewards().containsKey("OTHER"));

        assertNotNull(after.getInitiativeRejectionReasons());
        assertFalse(after.getInitiativeRejectionReasons().containsKey(INITIATIVE_ID));
        assertTrue(after.getInitiativeRejectionReasons().containsKey("OTHER"));
        assertEquals(List.of("R2"), after.getInitiativeRejectionReasons().get("OTHER"));
    }
}
