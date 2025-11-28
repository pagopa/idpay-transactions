package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.enums.SyncTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@ExtendWith(MockitoExtension.class)
class RewardTransactionServiceImplTest {
    @Mock
    private RewardTransactionRepository rewardTransactionRepository;

    @Mock
    private RewardBatchService rewardBatchService;

    private RewardTransactionService rewardTransactionService;
    @BeforeEach
    void setUp(){
        rewardTransactionService = new RewardTransactionServiceImpl(rewardTransactionRepository, rewardBatchService, 0x5a17beef);
    }

    @Test
    void findByIdTrxIssuer() {
        // Given
        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amountCents(3000L)
                .trxDate(LocalDateTime.of(2022, 9, 19, 15,43,39))
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.findByIdTrxIssuer(rt.getIdTrxIssuer(), null, null, null, null, null)).thenReturn(Flux.just(rt));

        // When
        Flux<RewardTransaction> result = rewardTransactionService.findByIdTrxIssuer("IDTRXISSUER", null, null, null, null, null);
        Assertions.assertNotNull(result);
        RewardTransaction resultRT = result.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(resultRT);
        Assertions.assertEquals(rt, resultRT);
    }

    @Test
    void findByRange() {
        // Given
        LocalDateTime date = LocalDateTime.of(2022, 9, 19, 15,43,39);
        LocalDateTime startDate = date.minusMonths(9L);
        LocalDateTime endDate = date.plusMonths(6L);


        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amountCents(3000L)
                .trxDate(date)
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.findByRange(rt.getUserId(), startDate, endDate, null, null)).thenReturn(Flux.just(rt));

        // When
        Flux<RewardTransaction> result = rewardTransactionService.findByRange(rt.getUserId(), startDate, endDate, null, null);
        Assertions.assertNotNull(result);
        RewardTransaction resultRT = result.toStream().findFirst().orElse(null);
        Assertions.assertNotNull(resultRT);
        Assertions.assertEquals(rt, resultRT);
    }

    @Test
    void save(){
        // Given
        RewardTransaction rt = RewardTransaction.builder()
                .userId("USERID")
                .amountCents(3000L)
                .trxDate(LocalDateTime.of(2022, 9, 19, 15,43,39))
                .idTrxIssuer("IDTRXISSUER")
                .build();

        Mockito.when(rewardTransactionRepository.save(rt)).thenReturn(Mono.just(rt));

        //when
        RewardTransaction result = rewardTransactionService.save(rt).block();

        //Then
        Assertions.assertNotNull(result);
        Assertions.assertEquals(rt, result);
        Mockito.verifyNoMoreInteractions(rewardTransactionRepository);
    }

    @Test
    void save_invoiced_enrichesBatch() {
        RewardTransaction rt = RewardTransaction.builder()
            .id("TRX_ID")
            .userId("USERID")
            .amountCents(3000L)
            .trxDate(LocalDateTime.of(2022, 9, 19, 15, 43, 39))
            .idTrxIssuer("IDTRXISSUER")
            .status("INVOICED")
            .merchantId("MERCHANT1")
            .pointOfSaleType(PosType.ONLINE)
            .pointOfSaleId("POS1")
            .businessName("Test Business")
            .trxChargeDate(LocalDateTime.of(2025, 11, 19, 15, 43, 39))
            .rewards(Map.of("initiative1", Reward.builder().accruedRewardCents(1000L).build()))
            .initiatives(List.of("initiative1"))
            .status(SyncTrxStatus.INVOICED.name())
            .build();

        RewardBatch batch = new RewardBatch();
        batch.setId("BATCH1");

        Mockito.when(rewardBatchService.findOrCreateBatch(
            rt.getMerchantId(),
            rt.getPointOfSaleType(),
            "2025-11",
            rt.getBusinessName()
        )).thenReturn(Mono.just(batch));

        Mockito.when(rewardBatchService.incrementTotals(batch.getId(), 1000L))
            .thenReturn(Mono.just(batch));

        Mockito.when(rewardTransactionRepository.save(Mockito.any()))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        RewardTransaction result = rewardTransactionService.save(rt).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals("BATCH1", result.getRewardBatchId());
        Assertions.assertEquals(RewardBatchTrxStatus.CONSULTABLE, result.getRewardBatchTrxStatus());
        Assertions.assertNotNull(result.getRewardBatchInclusionDate());
        Mockito.verify(rewardTransactionRepository, Mockito.times(1)).save(Mockito.any());
    }

    @Test
    void computeSamplingKey_shouldBeDeterministicForSameInput() {

        String id = "6543e5b9d9f31b0d94f6d21c";

        int h1 = ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey(id);
        int h2 = ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey(id);
        int h3 = ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey(id);

        // Must always match
        Assertions.assertEquals(h1, h2);
        Assertions.assertEquals(h1, h3);
    }

    @Test
    void computeSamplingKey_shouldDifferForDifferentIds() {

        int h1 = ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey("a123");
        int h2 = ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey("b456");

        Assertions.assertNotEquals(h1, h2, "Different IDs should normally yield different hashes");
    }

    @Test
    void computeSamplingKey_shouldChangeWhenSeedChanges() {
        String id = "6543e5b9d9f31b0d94f6d21c";
        RewardTransactionServiceImpl hasher2 = new RewardTransactionServiceImpl(rewardTransactionRepository, rewardBatchService, 0x22222222);

        int h1 = ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey(id);
        int h2 = hasher2.computeSamplingKey(id);

        Assertions.assertNotEquals(h1, h2,
                "Changing the seed must change the resulting sampling key");
    }

    @Test
    void computeSamplingKey_shouldHandleEmptyString() {
        int h = ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey(StringUtils.EMPTY);
        // Not null and deterministic
        Assertions.assertEquals(h, ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey(StringUtils.EMPTY));
    }

    @Test
    void computeSamplingKey_shouldThrowOnNullId() {

        Assertions.assertThrows(NullPointerException.class, () -> {
            ((RewardTransactionServiceImpl)rewardTransactionService).computeSamplingKey(null);
        });
    }
}
