package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.dto.ReasonDTO;
import it.gov.pagopa.idpay.transactions.dto.TrxFiltersDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.ChecksError;
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
    void updateStatusAndReturnOld_success() {
        String trxId = "TRX_ID_UPDATE";
        String batchMonth = "2024-01";
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setId(trxId);
        trx.setRewardBatchId(BATCH_ID);
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        rewardTransactionRepository.save(trx).block();

        ReasonDTO reasons = new ReasonDTO(LocalDateTime.now(), "REJECTION_REASON");

        StepVerifier.create(rewardTransactionSpecificRepository.updateStatusAndReturnOld(
                        BATCH_ID, trxId, RewardBatchTrxStatus.REJECTED, reasons, batchMonth, null))
                .assertNext(oldTrx -> {
                    assertEquals(RewardBatchTrxStatus.CONSULTABLE, oldTrx.getRewardBatchTrxStatus());
                    assertEquals(trxId, oldTrx.getId());
                })
                .verifyComplete();

        RewardTransaction updatedTrx = rewardTransactionRepository.findById(trxId).block();
        assertNotNull(updatedTrx);
        assertEquals(RewardBatchTrxStatus.REJECTED, updatedTrx.getRewardBatchTrxStatus());
        assertEquals(batchMonth, updatedTrx.getRewardBatchLastMonthElaborated());
        assertNotNull(updatedTrx.getRewardBatchRejectionReason());
        assertEquals("REJECTION_REASON", updatedTrx.getRewardBatchRejectionReason().getFirst().getReason());

        StepVerifier.create(rewardTransactionSpecificRepository.updateStatusAndReturnOld(
                        BATCH_ID, trxId, RewardBatchTrxStatus.SUSPENDED, null, batchMonth, null))
                .expectNextCount(1)
                .verifyComplete();

        RewardTransaction unsetTrx = rewardTransactionRepository.findById(trxId).block();
        assertNotNull(unsetTrx);
        assertEquals(RewardBatchTrxStatus.SUSPENDED, unsetTrx.getRewardBatchTrxStatus());
        assertNull(unsetTrx.getRewardBatchRejectionReason());
    }

    @Test
    void updateStatusAndReturnOld_notFound() {
        StepVerifier.create(rewardTransactionSpecificRepository.updateStatusAndReturnOld(
                        "NON_EXISTENT_BATCH", "NON_EXISTENT_ID",
                        RewardBatchTrxStatus.REJECTED, null, "2024-01", null))
                .expectNextCount(0)
                .verifyComplete();
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

    @Test
    void findByIdTrxIssuer_shouldFilterByAllFields() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction match = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("m1")
                .idTrxIssuer("ISSUER1")
                .userId(USER_ID)
                .amountCents(100L)
                .trxDate(now.minusHours(1))
                .build();

        RewardTransaction noUser = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("m2")
                .idTrxIssuer("ISSUER1")
                .userId("OTHER")
                .amountCents(100L)
                .trxDate(now.minusHours(1))
                .build();

        rewardTransactionRepository.saveAll(List.of(match, noUser)).collectList().block();

        List<String> ids = rewardTransactionSpecificRepository
                .findByIdTrxIssuer("ISSUER1", USER_ID, now.minusDays(1), now, 100L, PageRequest.of(0, 10))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(List.of("m1"), ids);
    }

    @Test
    void findByIdTrxIssuer_withOnlyStartDate_shouldFilter() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction in = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("in")
                .idTrxIssuer("ISSUER2")
                .trxDate(now.minusHours(1))
                .build();

        RewardTransaction out = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("out")
                .idTrxIssuer("ISSUER2")
                .trxDate(now.minusDays(3))
                .build();

        rewardTransactionRepository.saveAll(List.of(in, out)).collectList().block();

        List<String> ids = rewardTransactionSpecificRepository
                .findByIdTrxIssuer("ISSUER2", null, now.minusDays(1), null, null, PageRequest.of(0, 10))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(List.of("in"), ids);
    }

    @Test
    void findByIdTrxIssuer_withOnlyEndDate_shouldFilter() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction in = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("in")
                .idTrxIssuer("ISSUER3")
                .trxDate(now.minusDays(2))
                .build();

        RewardTransaction out = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("out")
                .idTrxIssuer("ISSUER3")
                .trxDate(now.plusDays(1))
                .build();

        rewardTransactionRepository.saveAll(List.of(in, out)).collectList().block();

        List<String> ids = rewardTransactionSpecificRepository
                .findByIdTrxIssuer("ISSUER3", null, null, now, null, PageRequest.of(0, 10))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(List.of("in"), ids);
    }

    @Test
    void findByRange_shouldFilterByDateAndAmount() {
        LocalDateTime now = LocalDateTime.now();

        RewardTransaction match = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("r1")
                .userId(USER_ID)
                .amountCents(500L)
                .trxDate(now.minusHours(1))
                .build();

        RewardTransaction noAmount = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("r2")
                .userId(USER_ID)
                .amountCents(400L)
                .trxDate(now.minusHours(1))
                .build();

        rewardTransactionRepository.saveAll(List.of(match, noAmount)).collectList().block();

        List<String> ids = rewardTransactionSpecificRepository
                .findByRange(USER_ID, now.minusDays(1), now, 500L, PageRequest.of(0, 10))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(List.of("r1"), ids);
    }

    @Test
    void findByFilter_rewardBatchAndStatuses_shouldReturnMatchingOnly() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("t1")
                .rewardBatchId(BATCH_ID)
                .initiatives(List.of(INITIATIVE_ID))
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("t2")
                .rewardBatchId(BATCH_ID)
                .initiatives(List.of(INITIATIVE_ID))
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .build();

        RewardTransaction t3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("t3")
                .rewardBatchId("OTHER")
                .initiatives(List.of(INITIATIVE_ID))
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2, t3)).collectList().block();

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilter(BATCH_ID, INITIATIVE_ID, List.of(RewardBatchTrxStatus.SUSPENDED, RewardBatchTrxStatus.REJECTED))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertTrue(ids.contains("t1"));
        assertTrue(ids.contains("t2"));
        assertFalse(ids.contains("t3"));
    }

    @Test
    void findTransaction_shouldReturnOnlyAllowedStatuses() {
        RewardTransaction ok = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("ok")
                .merchantId(MERCHANT_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .build();

        RewardTransaction ko = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("ko")
                .merchantId(MERCHANT_ID)
                .status("AUTHORIZED")
                .build();

        rewardTransactionRepository.saveAll(List.of(ok, ko)).collectList().block();

        RewardTransaction found = rewardTransactionSpecificRepository.findTransaction(MERCHANT_ID, "ok").block();
        RewardTransaction notFound = rewardTransactionSpecificRepository.findTransaction(MERCHANT_ID, "ko").block();

        assertNotNull(found);
        assertEquals("ok", found.getId());
        assertNull(notFound);
    }

    @Test
    void getCount_shouldCountMatchingTransactions() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("c1")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("c2")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(MERCHANT_ID, INITIATIVE_ID, null,
                SyncTrxStatus.REWARDED.name(), null, null, null, null);

        Long count = rewardTransactionSpecificRepository
                .getCount(filters, POS_ID, "", USER_ID, false)
                .block();

        assertEquals(2L, count);
    }

    @Test
    void findOneByInitiativeId_shouldReturnOne() {
        RewardTransaction trx = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("one")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.save(trx).block();

        RewardTransaction found = rewardTransactionSpecificRepository.findOneByInitiativeId(INITIATIVE_ID).block();

        assertNotNull(found);
        assertEquals("one", found.getId());
    }

    @Test
    void findByInitiativesWithBatch_shouldReturnTransactions() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("b1")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("b2")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2)).collectList().block();

        List<String> ids = rewardTransactionSpecificRepository
                .findByInitiativesWithBatch(INITIATIVE_ID, 1)
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(2, ids.size());
        assertTrue(ids.contains("b1"));
        assertTrue(ids.contains("b2"));
    }

    @Test
    void findByInitiativeIdAndUserId_shouldFilterBothFields() {
        RewardTransaction match = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("u1")
                .userId(USER_ID)
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction other = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("u2")
                .userId("OTHER")
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(match, other)).collectList().block();

        List<String> ids = rewardTransactionSpecificRepository
                .findByInitiativeIdAndUserId(INITIATIVE_ID, USER_ID)
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(List.of("u1"), ids);
    }

    @Test
    void sumSuspendedAccruedRewardCents_shouldSumValues() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("s1")
                .rewardBatchId(BATCH_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(Map.of(
                        "A", Reward.builder().accruedRewardCents(100L).build(),
                        "B", Reward.builder().accruedRewardCents(50L).build()
                ))
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("s2")
                .rewardBatchId(BATCH_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(Map.of(
                        "C", Reward.builder().accruedRewardCents(30L).build()
                ))
                .build();

        RewardTransaction t3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("s3")
                .rewardBatchId(BATCH_ID)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(
                        "D", Reward.builder().accruedRewardCents(999L).build()
                ))
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2, t3)).collectList().block();

        Long sum = rewardTransactionSpecificRepository.sumSuspendedAccruedRewardCents(BATCH_ID).block();

        assertEquals(180L, sum);
    }

    @Test
    void updateStatusAndReturnOld_withChecksError_shouldSetChecksError() {
        String trxId = "trx-checks";
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setId(trxId);
        trx.setRewardBatchId(BATCH_ID);
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        rewardTransactionRepository.save(trx).block();

        ChecksError checksError = new ChecksError(
                true,   // cfError
                false,  // productEligibilityError
                false,  // disposalRaeeError
                false,  // priceError
                false,  // bonusError
                false,  // sellerReferenceError
                false,  // accountingDocumentError
                true    // genericError
        );

        StepVerifier.create(rewardTransactionSpecificRepository.updateStatusAndReturnOld(
                        BATCH_ID, trxId, RewardBatchTrxStatus.REJECTED, null, "2024-01", checksError))
                .expectNextCount(1)
                .verifyComplete();

        RewardTransaction updated = rewardTransactionRepository.findById(trxId).block();
        assertNotNull(updated);
        assertNotNull(updated.getChecksError());
        assertEquals(checksError, updated.getChecksError());
        assertTrue(updated.getChecksError().isCfError());
        assertTrue(updated.getChecksError().isGenericError());
    }

    @Test
    void updateStatusAndReturnOld_sameStatus_shouldAppendReasons() {
        String trxId = "trx-reasons";
        RewardTransaction trx = RewardTransactionFaker.mockInstance(1);
        trx.setId(trxId);
        trx.setRewardBatchId(BATCH_ID);
        trx.setRewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED);
        trx.setRewardBatchRejectionReason(List.of(new ReasonDTO(LocalDateTime.now(), "OLD")));
        rewardTransactionRepository.save(trx).block();

        ReasonDTO newReason = new ReasonDTO(LocalDateTime.now(), "NEW");

        StepVerifier.create(rewardTransactionSpecificRepository.updateStatusAndReturnOld(
                        BATCH_ID, trxId, RewardBatchTrxStatus.REJECTED, newReason, "2024-01", null))
                .expectNextCount(1)
                .verifyComplete();

        RewardTransaction updated = rewardTransactionRepository.findById(trxId).block();
        assertNotNull(updated);
        assertNotNull(updated.getRewardBatchRejectionReason());
        assertEquals(2, updated.getRewardBatchRejectionReason().size());
    }

    @Test
    void findInvoicedTrxByIdWithoutBatch_shouldReturnOnlyMatching() {
        RewardTransaction ok = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("ib1")
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchId(null)
                .build();

        RewardTransaction ko = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("ib2")
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchId(BATCH_ID)
                .build();

        rewardTransactionRepository.saveAll(List.of(ok, ko)).collectList().block();

        RewardTransaction found = rewardTransactionSpecificRepository.findInvoicedTrxByIdWithoutBatch("ib1").block();
        RewardTransaction notFound = rewardTransactionSpecificRepository.findInvoicedTrxByIdWithoutBatch("ib2").block();

        assertNotNull(found);
        assertEquals("ib1", found.getId());
        assertNull(notFound);
    }

    @Test
    void findDistinctFranchiseAndPosByRewardBatchId_shouldReturnDistinctPairs() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("d1")
                .rewardBatchId(BATCH_ID)
                .franchiseName("F1")
                .pointOfSaleId("P1")
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("d2")
                .rewardBatchId(BATCH_ID)
                .franchiseName("F1")
                .pointOfSaleId("P1")
                .build();

        RewardTransaction t3 = RewardTransactionFaker.mockInstanceBuilder(3)
                .id("d3")
                .rewardBatchId(BATCH_ID)
                .franchiseName("F2")
                .pointOfSaleId("P2")
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2, t3)).collectList().block();

        List<it.gov.pagopa.idpay.transactions.dto.FranchisePointOfSaleDTO> out =
                rewardTransactionSpecificRepository.findDistinctFranchiseAndPosByRewardBatchId(BATCH_ID)
                        .collectList()
                        .block();

        assertNotNull(out);
        assertEquals(2, out.size());
    }

    @Test
    void findTransactionInBatch_shouldReturnNullWhenMissing() {
        RewardTransaction found = rewardTransactionSpecificRepository
                .findTransactionInBatch(MERCHANT_ID, BATCH_ID, "missing")
                .block();

        assertNull(found);
    }

    @Test
    void findByFilterTrx_withTrxCode_shouldFilterByRegex() {
        RewardTransaction t1 = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("tc1")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .trxCode("ABC-123")
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction t2 = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("tc2")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .trxCode("ZZZ")
                .status(SyncTrxStatus.REWARDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(t1, t2)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO();
        filters.setMerchantId(MERCHANT_ID);
        filters.setInitiativeId(INITIATIVE_ID);
        filters.setStatus(SyncTrxStatus.REWARDED.name());
        filters.setTrxCode("123");

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilterTrx(filters, POS_ID, USER_ID, "", false, PageRequest.of(0, 10))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(List.of("tc1"), ids);
    }

    @Test
    void findByFilter_withConsultableAndIncludeToCheck_shouldReturnBothStatuses() {
        RewardTransaction consultable = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("co")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction toCheck = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("tc")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .status(SyncTrxStatus.INVOICED.name())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(consultable, toCheck)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(
                MERCHANT_ID,
                INITIATIVE_ID,
                null,
                SyncTrxStatus.INVOICED.name(),
                null,
                RewardBatchTrxStatus.CONSULTABLE,
                null,
                null
        );

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilter(filters, USER_ID, true, PageRequest.of(0, 10))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(2, ids.size());
        assertTrue(ids.contains("co"));
        assertTrue(ids.contains("tc"));
    }

    @Test
    void findByFilterTrx_sortByStatusDesc_shouldReverseRankOrder() {
        RewardTransaction c = RewardTransactionFaker.mockInstanceBuilder(1)
                .id("c")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.CANCELLED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        RewardTransaction rf = RewardTransactionFaker.mockInstanceBuilder(2)
                .id("rf")
                .merchantId(MERCHANT_ID)
                .userId(USER_ID)
                .pointOfSaleId(POS_ID)
                .status(SyncTrxStatus.REFUNDED.name())
                .initiatives(List.of(INITIATIVE_ID))
                .build();

        rewardTransactionRepository.saveAll(List.of(c, rf)).collectList().block();

        TrxFiltersDTO filters = new TrxFiltersDTO(MERCHANT_ID, INITIATIVE_ID, null, null, null, null, null, null);

        List<String> ids = rewardTransactionSpecificRepository
                .findByFilterTrx(filters, POS_ID, USER_ID, "", false,
                        PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "status")))
                .map(RewardTransaction::getId)
                .collectList()
                .block();

        assertEquals(List.of("rf", "c"), ids);
    }
}
