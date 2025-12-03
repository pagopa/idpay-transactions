package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.ClientExceptionWithBody;
import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.common.web.exception.RewardBatchNotFound;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RewardBatchServiceImplTest {

  @Mock
  private RewardBatchRepository rewardBatchRepository;
  @Mock
  private RewardTransactionRepository rewardTransactionRepository;

  private RewardBatchServiceImpl rewardBatchService;
  private RewardBatchServiceImpl rewardBatchServiceSpy;

  private static final String BUSINESS_NAME = "Test Business name";
  private static final String REWARD_BATCH_ID = "REWARD_BATCH_ID";
  private static final String REWARD_BATCH_ID_NEW = "REWARD_BATCH_ID_NEW";
  private static final String INITIATIVE_ID = "INITIATIVE_ID";
  private static final  RewardBatch REWARD_BATCH_OLD = RewardBatch.builder()
            .id(REWARD_BATCH_ID)
            .posType(PosType.PHYSICAL)
            .status(RewardBatchStatus.CREATED)
            .build();

    private static final  RewardBatch REWARD_BATCH_NEW = RewardBatch.builder()
            .id(REWARD_BATCH_ID_NEW)
            .posType(PosType.PHYSICAL)
            .status(RewardBatchStatus.CREATED)
            .build();




  @BeforeEach
  void setUp() {
    rewardBatchService = new RewardBatchServiceImpl(rewardBatchRepository,
        rewardTransactionRepository);
    rewardBatchServiceSpy = spy((RewardBatchServiceImpl) rewardBatchService);
  }


  @Test
  void findOrCreateBatch_createsNewBatch() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PosType.PHYSICAL, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == PosType.PHYSICAL;
          assert batch.getStatus() == RewardBatchStatus.CREATED;
          assert batch.getName().contains("novembre 2025");
          assert batch.getStartDate().equals(yearMonth.atDay(1).atStartOfDay());
          assert batch.getEndDate().equals(yearMonth.atEndOfMonth().atTime(23, 59, 59));
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).save(any());
  }

  @Test
  void findOrCreateBatch_existingBatch() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    RewardBatch existingBatch = RewardBatch.builder()
        .id("BATCH1")
        .merchantId("M1")
        .posType(PosType.PHYSICAL)
        .month(batchMonth)
        .status(RewardBatchStatus.CREATED)
        .name("novembre 2025")
        .build();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PosType.PHYSICAL,
            batchMonth))
        .thenReturn(Mono.just(existingBatch));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == PosType.PHYSICAL;
          assert batch.getStatus() == RewardBatchStatus.CREATED;
          assert batch.getName().contains("novembre 2025");
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository, Mockito.never()).save(any());
  }

  @Test
  void findOrCreateBatch_handlesDuplicateKeyException() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();
    PosType posType = PosType.PHYSICAL;

    RewardBatch existingBatch = RewardBatch.builder()
        .id("BATCH_DUP")
        .merchantId("M1")
        .posType(posType)
        .month(batchMonth)
        .status(RewardBatchStatus.CREATED)
        .name("novembre 2025")
        .build();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", posType, batchMonth))
        .thenReturn(Mono.empty())
        .thenReturn(Mono.just(existingBatch));

    Mockito.when(rewardBatchRepository.save(any()))
        .thenReturn(Mono.error(new DuplicateKeyException("Duplicate")));

    StepVerifier.create(
            new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionRepository)
                .findOrCreateBatch("M1", posType, batchMonth, BUSINESS_NAME)
        )
        .assertNext(batch -> {
          assert batch.getId().equals("BATCH_DUP");
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == posType;
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository, Mockito.times(2))
        .findByMerchantIdAndPosTypeAndMonth("M1", posType, batchMonth);
    Mockito.verify(rewardBatchRepository).save(any());
  }

  @Test
  void buildBatchName_physicalPos() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PosType.PHYSICAL, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getName().contains("novembre 2025");
        })
        .verifyComplete();
  }

  @Test
  void buildBatchName_onlinePos() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PosType.ONLINE, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getName().contains("novembre 2025");
        })
        .verifyComplete();
  }

  @Test
  void buildBatchName_baseName() {
    YearMonth yearMonth = YearMonth.of(2025, 11);
    String batchMonth = yearMonth.toString();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth(
            "M1", PosType.ONLINE, batchMonth))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(
            rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getName().equals("novembre 2025");
        })
        .verifyComplete();
  }

  @Test
  void getRewardBatches_forMerchant_returnsPagedResult() {
    String merchantId = "M1";
    String organizationRole = null;
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch rb1 = RewardBatch.builder().id("B1").merchantId(merchantId).name("novembre 2025").build();
    RewardBatch rb2 = RewardBatch.builder().id("B2").merchantId(merchantId).name("novembre 2025").build();

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, false, pageable)
    ).thenReturn(Flux.just(rb1, rb2));

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, false)
    ).thenReturn(Mono.just(5L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel, pageable)
        )
        .assertNext(page -> {
          assertEquals(2, page.getContent().size());
          assertEquals("B1", page.getContent().get(0).getId());
          assertEquals("B2", page.getContent().get(1).getId());
          assertEquals(5, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();
  }

  @Test
  void getRewardBatches_forMerchant_emptyPage() {
    String merchantId = "M1";
    String organizationRole = null;
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(1, 2);

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, false, pageable)
    ).thenReturn(Flux.empty());

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, false)
    ).thenReturn(Mono.just(0L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel, pageable)
        )
        .assertNext(page -> {
          assertTrue(page.getContent().isEmpty());
          assertEquals(0, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();
  }

  @Test
  void getRewardBatches_forOperator_returnsPagedResult() {
    String merchantId = null;
    String organizationRole = "operator1";
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch batchA = RewardBatch.builder().id("B1").merchantId("MERCHANT1").name("novembre 2025").build();
    RewardBatch batchB = RewardBatch.builder().id("B2").merchantId("MERCHANT2").name("novembre 2025").build();

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, true, pageable)
    ).thenReturn(Flux.just(batchA, batchB));

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, true)
    ).thenReturn(Mono.just(10L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel, pageable)
        )
        .assertNext(page -> {
          assertEquals(2, page.getContent().size());
          assertEquals(10, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).findRewardBatchesCombined(merchantId, status, assigneeLevel, true, pageable);
    Mockito.verify(rewardBatchRepository).getCountCombined(merchantId, status, assigneeLevel, true);
  }

  @Test
  void getRewardBatches_forOperator_emptyPage() {
    String merchantId = null;
    String organizationRole = "operator1";
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(0, 2);

    Mockito.when(
        rewardBatchRepository.findRewardBatchesCombined(merchantId, status, assigneeLevel, true, pageable)
    ).thenReturn(Flux.empty());

    Mockito.when(
        rewardBatchRepository.getCountCombined(merchantId, status, assigneeLevel, true)
    ).thenReturn(Mono.just(0L));

    StepVerifier.create(
            rewardBatchService.getRewardBatches(merchantId, organizationRole, status, assigneeLevel, pageable)
        )
        .assertNext(page -> {
          assertTrue(page.getContent().isEmpty());
          assertEquals(0, page.getTotalElements());
          assertEquals(pageable, page.getPageable());
        })
        .verifyComplete();
  }

  @Test
  void incrementTotals_callsRepository() {
    RewardBatch updated = RewardBatch.builder()
        .id("B1")
        .initialAmountCents(500L)
        .build();

    Mockito.when(rewardBatchRepository.incrementTotals("B1", 200L))
        .thenReturn(Mono.just(updated));

    StepVerifier.create(rewardBatchService.incrementTotals("B1", 200L))
        .expectNextMatches(b -> b.getInitialAmountCents() == 500L)
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).incrementTotals("B1", 200L);
  }

  @Test
  void sendRewardBatch_batchNotFound() {
    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.empty());

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectError(RewardBatchException.class)
        .verify();
  }

  @Test
  void sendRewardBatch_merchantIdMismatch() {
    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("OTHER")
        .month("2025-11")
        .status(RewardBatchStatus.CREATED)
        .build();

    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectError(RewardBatchException.class)
        .verify();
  }

  @Test
  void sendRewardBatch_invalidStatus() {
    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month("2025-11")
        .status(RewardBatchStatus.SENT)
        .build();

    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectError(RewardBatchException.class)
        .verify();
  }

  @Test
  void sendRewardBatch_monthTooEarly() {
    YearMonth now = YearMonth.now();

    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month(now.toString())
        .status(RewardBatchStatus.CREATED)
        .build();

    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.just(batch));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .expectError(RewardBatchException.class)
        .verify();
  }

  @Test
  void sendRewardBatch_success() {
    YearMonth oldMonth = YearMonth.now().minusMonths(2);

    RewardBatch batch = RewardBatch.builder()
        .id("B1")
        .merchantId("M1")
        .month(oldMonth.toString())
        .status(RewardBatchStatus.CREATED)
        .build();

    Mockito.when(rewardBatchRepository.findById("B1"))
        .thenReturn(Mono.just(batch));

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(inv -> Mono.just(inv.getArgument(0)));

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).save(any());
    Mockito.verify(rewardTransactionRepository, never()).rewardTransactionsByBatchId(any());
  }

  @Test
  void isOperator_shouldReturnFalse_whenRoleIsNull() throws Exception {
    var method = RewardBatchServiceImpl.class.getDeclaredMethod("isOperator", String.class);
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(rewardBatchService, (String) null);
    assertFalse(result);
  }

  @Test
  void isOperator_shouldReturnFalse_whenRoleIsNotOperator() throws Exception {
    var method = RewardBatchServiceImpl.class.getDeclaredMethod("isOperator", String.class);
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(rewardBatchService, "randomRole");
    assertFalse(result);
  }

    @Test
    void suspendTransactions_ok() {
        String batchId = "BATCH1";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("TX1", "TX2"));
        request.setReason("Check");

        RewardTransaction oldTx1 = new RewardTransaction();
        oldTx1.setId("TX1");
        oldTx1.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
        oldTx1.setRewards(Map.of(
                initiativeId,
                Reward.builder().accruedRewardCents(100L).build()
        ));

        RewardTransaction oldTx2 = new RewardTransaction();
        oldTx2.setId("TX2");
        oldTx2.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
        oldTx2.setRewards(Map.of(
                initiativeId,
                Reward.builder().accruedRewardCents(200L).build()
        ));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "TX1", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(oldTx1));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "TX2", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(oldTx2));

        long expectedElaborated = 1L;
        long expectedSuspended = 2L;
        long expectedApprovedAmount = -300L;
        long expectedRejected = 0L;

        when(rewardBatchRepository.updateTotals(batchId, expectedElaborated, expectedApprovedAmount, expectedRejected, expectedSuspended))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNextMatches(result -> result.getId().equals(batchId)
                        && result.getStatus() == RewardBatchStatus.EVALUATING)
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(batchId, expectedElaborated, expectedApprovedAmount, expectedRejected, expectedSuspended);
    }

    @Test
    void suspendTransactions_noModifiedTransactions() {
        String batchId = "BATCH1";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("TX3", "TX4"));
        request.setReason("Check");

        RewardTransaction oldTx = new RewardTransaction();
        oldTx.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
        oldTx.setRewards(Map.of());

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(batchId), anyString(), eq(RewardBatchTrxStatus.SUSPENDED), eq(request.getReason())))
                .thenReturn(Mono.just(oldTx));

        when(rewardBatchRepository.updateTotals(batchId, 0L, 0L, 0L, 0L))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(batchId, 0L, 0L, 0L, 0L);
    }

    @Test
    void suspendTransactions_suspendedTotalZero_returnsOriginalBatch() {
        String batchId = "batch123";
        String initiativeId = "init123";

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1", "trx2"));

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        RewardTransaction oldTx = new RewardTransaction();
        oldTx.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
        oldTx.setRewards(Map.of(initiativeId,
                Reward.builder().accruedRewardCents(null).build()));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(batchId), anyString(), eq(RewardBatchTrxStatus.SUSPENDED), eq(request.getReason())))
                .thenReturn(Mono.just(oldTx));

        when(rewardBatchRepository.updateTotals(batchId, 2L, 0L, 0L, 2L))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(batchId, 2L, 0L, 0L, 2L);
    }

    @Test
    void suspendTransactions_throwsException_whenBatchApproved_serviceLevel() {
        String batchId = "BATCH_APPROVED";
        String initiativeId = "INIT1";

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1"));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectErrorMatches(ex ->
                        ex instanceof ClientExceptionWithBody &&
                                ex.getMessage().replaceAll("\\s+", " ")
                                        .contains("Reward batch BATCH_APPROVED not found or not in a valid state")
                )
                .verify();

        verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(), any(), any());
        verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void suspendTransactions_handlesRejectedTransaction() {
        String batchId = "batchRejected";
        String initiativeId = "initRejected";

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1"));

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        RewardTransaction rejectedTx = new RewardTransaction();
        rejectedTx.setRewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED);
        rejectedTx.setRewards(Map.of(initiativeId,
                Reward.builder().accruedRewardCents(100L).build()));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardTransactionRepository.updateStatusAndReturnOld(eq(batchId), anyString(), eq(RewardBatchTrxStatus.SUSPENDED), eq(request.getReason())))
                .thenReturn(Mono.just(rejectedTx));

        when(rewardBatchRepository.updateTotals(batchId, 0L, 0L, -1L, 1L))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(batchId, 0L, 0L, -1L, 1L);
    }

    @Test
    void suspendTransactions_handlesNullAndMissingRewards() {
        String batchId = "batchNullRewards";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trxNull", "trxMissing", "trxWithReward"));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxNull", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.empty());

        RewardTransaction trxMissing = new RewardTransaction();
        trxMissing.setId("trxMissing");
        trxMissing.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
        trxMissing.setRewards(Map.of("OTHER_INIT", Reward.builder().accruedRewardCents(50L).build()));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxMissing", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxMissing));

        RewardTransaction trxWithReward = new RewardTransaction();
        trxWithReward.setId("trxWithReward");
        trxWithReward.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
        trxWithReward.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(100L).build()));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxWithReward", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxWithReward));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTotals(batchId, 0L, -100L, 0L, 2L))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository).updateTotals(batchId, 0L, -100L, 0L, 2L);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(batchId, "trxNull", RewardBatchTrxStatus.SUSPENDED, request.getReason());
        verify(rewardTransactionRepository).updateStatusAndReturnOld(batchId, "trxMissing", RewardBatchTrxStatus.SUSPENDED, request.getReason());
        verify(rewardTransactionRepository).updateStatusAndReturnOld(batchId, "trxWithReward", RewardBatchTrxStatus.SUSPENDED, request.getReason());
    }

    @Test
    void suspendTransactions_trxSuspended() {
        String batchId = "batchAllStatuses";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trxSuspended"));

        RewardTransaction trxSuspended = new RewardTransaction();
        trxSuspended.setId("trxSuspended");
        trxSuspended.setRewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED);
        trxSuspended.setRewards(Map.of());

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxSuspended", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxSuspended));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void suspendTransactions_trxApproved() {
        String batchId = "batchAllStatuses";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trxApproved"));

        RewardTransaction trxApproved = new RewardTransaction();
        trxApproved.setId("trxApproved");
        trxApproved.setRewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED);
        trxApproved.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(100L).build()));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxApproved", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxApproved));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void suspendTransactions_mixedStatuses() {
        String batchId = "batchAllStatuses";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trxToCheck", "trxConsultable", "trxRejected"));

        RewardTransaction trxToCheck = new RewardTransaction();
        trxToCheck.setId("trxToCheck");
        trxToCheck.setRewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK);
        trxToCheck.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(null).build()));

        RewardTransaction trxConsultable = new RewardTransaction();
        trxConsultable.setId("trxConsultable");
        trxConsultable.setRewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE);
        trxConsultable.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(50L).build()));

        RewardTransaction trxRejected = new RewardTransaction();
        trxRejected.setId("trxRejected");
        trxRejected.setRewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED);
        trxRejected.setRewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(20L).build()));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxToCheck", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxToCheck));
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxConsultable", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxConsultable));
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxRejected", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxRejected));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void suspendTransactions_trxOldIsNull() {
        String batchId = "batchOldNull";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trxNull"));

        RewardTransaction trxNull = RewardTransaction.builder()
                .id(null)
                .rewards(Collections.emptyMap())
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .build();

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxNull", RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                .thenReturn(Mono.just(trxNull));

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void suspendTransactions_eachSwitchCase() {
        String batchId = "batchSwitch";
        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.EVALUATING)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trxApproved", "trxToCheck", "trxConsultable", "trxRejected", "trxSuspended"));

        RewardTransaction trxApproved = RewardTransaction.builder()
                .id("trxApproved")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(100L).build()))
                .build();

        RewardTransaction trxToCheck = RewardTransaction.builder()
                .id("trxToCheck")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(50L).build()))
                .build();

        RewardTransaction trxConsultable = RewardTransaction.builder()
                .id("trxConsultable")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(25L).build()))
                .build();

        RewardTransaction trxRejected = RewardTransaction.builder()
                .id("trxRejected")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(Map.of(initiativeId, Reward.builder().accruedRewardCents(10L).build()))
                .build();

        RewardTransaction trxSuspended = RewardTransaction.builder()
                .id("trxSuspended")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(Collections.emptyMap())
                .build();

        Map<String, RewardTransaction> trxMap = Map.of(
                "trxApproved", trxApproved,
                "trxToCheck", trxToCheck,
                "trxConsultable", trxConsultable,
                "trxRejected", trxRejected,
                "trxSuspended", trxSuspended
        );

        trxMap.forEach((id, trx) ->
                when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, id, RewardBatchTrxStatus.SUSPENDED, request.getReason()))
                        .thenReturn(Mono.just(trx))
        );

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTotals(eq(batchId), anyLong(), anyLong(), anyLong(), anyLong()))
                .thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();
    }

    @Test
    void rewardBatchConfirmation_Success_WithSuspendedTransactions() {
        REWARD_BATCH_OLD.setMonth(YearMonth.of(2025, 11).toString());
        REWARD_BATCH_OLD.setName("novembre 2025");
        REWARD_BATCH_OLD.setNumberOfTransactionsSuspended(10L);
        REWARD_BATCH_OLD.setStatus(RewardBatchStatus.EVALUATING);
        REWARD_BATCH_OLD.setAssigneeLevel(RewardBatchAssignee.L2);

        REWARD_BATCH_NEW.setMonth(YearMonth.of(2025, 12).toString());
        REWARD_BATCH_NEW.setName("dicembre 2025");

        when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID)).thenReturn(Mono.just(REWARD_BATCH_OLD));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenReturn(Mono.just(REWARD_BATCH_OLD));

        doReturn(Mono.empty()).when(rewardBatchServiceSpy).updateAndSaveRewardTransactionsToApprove(any(), any());
        doReturn(Mono.just(REWARD_BATCH_NEW)).when(rewardBatchServiceSpy).createRewardBatchAndSave(any());
        doReturn(Mono.empty()).when(rewardBatchServiceSpy).updateAndSaveRewardTransactionsSuspended(any(), any(), any());

        Mono<RewardBatch> result = rewardBatchServiceSpy.rewardBatchConfirmation(INITIATIVE_ID, REWARD_BATCH_ID);

            StepVerifier.create(result)
                    .expectNextMatches(batch ->
                            batch.getId().equals(REWARD_BATCH_ID_NEW) &&
                                    batch.getStatus().equals(RewardBatchStatus.CREATED)
                    )
                    .verifyComplete();

            verify(rewardBatchServiceSpy, times(1)).createRewardBatchAndSave(any());

    }

    @Test
    void rewardBatchConfirmation_Success_WithOutSuspendedTransactions() {
        REWARD_BATCH_OLD.setMonth(YearMonth.of(2025, 11).toString());
        REWARD_BATCH_OLD.setName("novembre 2025");
        REWARD_BATCH_OLD.setNumberOfTransactionsSuspended(0L);
        REWARD_BATCH_OLD.setStatus(RewardBatchStatus.EVALUATING);
        REWARD_BATCH_OLD.setAssigneeLevel(RewardBatchAssignee.L2);

        when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID)).thenReturn(Mono.just(REWARD_BATCH_OLD));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenReturn(Mono.just(REWARD_BATCH_OLD));

        doReturn(Mono.empty()).when(rewardBatchServiceSpy).updateAndSaveRewardTransactionsToApprove(any(), any());

        Mono<RewardBatch> result = rewardBatchServiceSpy.rewardBatchConfirmation(INITIATIVE_ID, REWARD_BATCH_ID);

        StepVerifier.create(result)
                .expectNextMatches(batch ->
                        batch.getId().equals(REWARD_BATCH_ID) &&
                                batch.getStatus().equals(RewardBatchStatus.APPROVED)
                )
                .verifyComplete();

        verify(rewardBatchServiceSpy, times(0)).createRewardBatchAndSave(any());

    }

    @Test
    void rewardBatchConfirmation_Failure_NotFound() {
        when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID)).thenReturn(Mono.empty());

        Mono<RewardBatch> result = rewardBatchServiceSpy.rewardBatchConfirmation(INITIATIVE_ID, REWARD_BATCH_ID);

        StepVerifier.create(result)
                .expectErrorMatches(throwable ->
                        throwable instanceof ClientExceptionWithBody &&
                                ((ClientExceptionWithBody) throwable).getHttpStatus().equals(HttpStatus.NOT_FOUND)
                )
                .verify();

        verify(rewardBatchRepository, times(0)).save(any());
        verify(rewardBatchServiceSpy, times(0)).createRewardBatchAndSave(any());
    }

    @Test
    void rewardBatchConfirmation_Failure_AlreadyApproved() {
        REWARD_BATCH_OLD.setStatus(RewardBatchStatus.APPROVED);
        when(rewardBatchRepository.findRewardBatchById(REWARD_BATCH_ID)).thenReturn(Mono.just(REWARD_BATCH_OLD));

        Mono<RewardBatch> result = rewardBatchServiceSpy.rewardBatchConfirmation(INITIATIVE_ID, REWARD_BATCH_ID);

        StepVerifier.create(result)
                .expectErrorMatches(throwable ->
                        throwable instanceof ClientExceptionWithBody &&
                                ((ClientExceptionWithBody) throwable).getHttpStatus().equals(HttpStatus.BAD_REQUEST)
                )
                .verify();

        verify(rewardBatchRepository, times(0)).save(any());
        verify(rewardBatchServiceSpy, times(0)).createRewardBatchAndSave(any());
    }

    @Test
    void approvedTransactions () {
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";

        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxApproved", "trxToCheck", "trxConsultable", "trxSuspended", "trxRejected")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING).build();

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(rewardBatch));

        //Mock for approved
        Reward rewardApproved = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, rewardApproved);
        RewardTransaction trxApprovedMock = RewardTransaction.builder()
                .id("trxApproved")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(rewardApprovedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxApproved",  RewardBatchTrxStatus.APPROVED, null))
                .thenReturn(Mono.just(trxApprovedMock));

        //Mock for to_check
        Reward rewardToCheck = Reward.builder().accruedRewardCents(2000L).build();
        Map<String, Reward> rewardToCheckMap = Map.of(initiativeId, rewardToCheck);
        RewardTransaction trxToCheckMock = RewardTransaction.builder()
                .id("trxToCheck")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(rewardToCheckMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxToCheck",  RewardBatchTrxStatus.APPROVED, null))
                .thenReturn(Mono.just(trxToCheckMock));

        //Mock for consultable
        Reward rewardConsultable = Reward.builder().accruedRewardCents(2500L).build();
        Map<String, Reward> rewardConsultableMap = Map.of(initiativeId, rewardConsultable);
        RewardTransaction trxConsultableMock = RewardTransaction.builder()
                .id("trxConsultable")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(rewardConsultableMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxConsultable",  RewardBatchTrxStatus.APPROVED, null))
                .thenReturn(Mono.just(trxConsultableMock));

        //Mock for suspended
        Reward rewardSuspended = Reward.builder().accruedRewardCents(3000L).build();
        Map<String, Reward> rewardSuspendedMap = Map.of(initiativeId, rewardSuspended);
        RewardTransaction trxSuspendedMock = RewardTransaction.builder()
                .id("trxSuspended")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(rewardSuspendedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxSuspended",  RewardBatchTrxStatus.APPROVED, null))
                .thenReturn(Mono.just(trxSuspendedMock));

        //Mock for rejected
        Reward rewardRejected = Reward.builder().accruedRewardCents(3500L).build();
        Map<String, Reward> rewardRejectedMap = Map.of(initiativeId, rewardRejected);
        RewardTransaction trxRejectedMock = RewardTransaction.builder()
                .id("trxRejected")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(rewardRejectedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxRejected",  RewardBatchTrxStatus.APPROVED, null))
                .thenReturn(Mono.just(trxRejectedMock));

        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                batchId,
                2L, //TO_CHECK and CONSULTABLE
                rewardSuspended.getAccruedRewardCents()+rewardRejected.getAccruedRewardCents(),
                -1L,
                -1L))
                .thenReturn(Mono.just(expectedResult));


        RewardBatch result = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);
        verify(rewardTransactionRepository, times(5)).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }

    @Test
    void approvedTransactions_NotFoundBatch(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());

        Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId);
        Assertions.assertThrows(ClientExceptionWithBody.class, resultMono::block);

        verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository, never()).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());

    }

    @Test
    void approvedTransactions_ErrorInUpdateInModifyTrx(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING).build();
        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(rewardBatch));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",  RewardBatchTrxStatus.APPROVED, null))
                .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId);
        Assertions.assertThrows(RuntimeException.class, resultMono::block);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository, never()).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }

    @Test
    void approvedTransactions_ErrorInUpdateBatch(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING).build();
        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(rewardBatch));

        //Mock for approved
        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewardBatchId(batchId)
                .rewards(rewardApprovedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",  RewardBatchTrxStatus.APPROVED, null))
                .thenReturn(Mono.just(trxMock));

        when(rewardBatchRepository.updateTotals(batchId,1L,  0L,0,0))
                .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId);
        Assertions.assertThrows(RuntimeException.class, resultMono::block);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }

    @Test
    void rejectTransactions () {
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";

        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxApproved", "trxToCheck", "trxConsultable", "trxSuspended", "trxRejected")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING).build();

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(rewardBatch));

        //Mock for approved
        Reward rewardApproved = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, rewardApproved);
        RewardTransaction trxApprovedMock = RewardTransaction.builder()
                .id("trxApproved")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(rewardApprovedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxApproved",  RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason()))
                .thenReturn(Mono.just(trxApprovedMock));

        //Mock for to_check
        Reward rewardToCheck = Reward.builder().accruedRewardCents(2000L).build();
        Map<String, Reward> rewardToCheckMap = Map.of(initiativeId, rewardToCheck);
        RewardTransaction trxToCheckMock = RewardTransaction.builder()
                .id("trxToCheck")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(rewardToCheckMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxToCheck",  RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason()))
                .thenReturn(Mono.just(trxToCheckMock));

        //Mock for consultable
        Reward rewardConsultable = Reward.builder().accruedRewardCents(2500L).build();
        Map<String, Reward> rewardConsultableMap = Map.of(initiativeId, rewardConsultable);
        RewardTransaction trxConsultableMock = RewardTransaction.builder()
                .id("trxConsultable")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(rewardConsultableMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxConsultable",  RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason()))
                .thenReturn(Mono.just(trxConsultableMock));

        //Mock for suspended
        Reward rewardSuspended = Reward.builder().accruedRewardCents(3000L).build();
        Map<String, Reward> rewardSuspendedMap = Map.of(initiativeId, rewardSuspended);
        RewardTransaction trxSuspendedMock = RewardTransaction.builder()
                .id("trxSuspended")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(rewardSuspendedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxSuspended",  RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason()))
                .thenReturn(Mono.just(trxSuspendedMock));

        //Mock for rejected
        Reward rewardRejected = Reward.builder().accruedRewardCents(3500L).build();
        Map<String, Reward> rewardRejectedMap = Map.of(initiativeId, rewardRejected);
        RewardTransaction trxRejectedMock = RewardTransaction.builder()
                .id("trxRejected")
                .rewardBatchId(batchId)
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(rewardRejectedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxRejected",  RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason()))
                .thenReturn(Mono.just(trxRejectedMock));

        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                batchId,
                2L, //TO_CHECK and CONSULTABLE
                -rewardApproved.getAccruedRewardCents()-rewardToCheck.getAccruedRewardCents()-rewardConsultable.getAccruedRewardCents(),
                4L,
                -1L))
                .thenReturn(Mono.just(expectedResult));


        RewardBatch result = rewardBatchService.rejectTransactions(batchId, initiativeId, transactionsRequest).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);
        verify(rewardTransactionRepository, times(5)).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }

    @Test
    void rejectTransactions_NotFoundBatch(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";

        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.empty());

        Mono<RewardBatch> resultMono = rewardBatchService.rejectTransactions(batchId, initiativeId, transactionsRequest);
        Assertions.assertThrows(ClientExceptionWithBody.class, resultMono::block);

        verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository, never()).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());

    }

    @Test
    void rejectTransactions_ErrorInUpdateInModifyTrx(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";

        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING).build();
        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(rewardBatch));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",  RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason()))
                .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        Mono<RewardBatch> resultMono = rewardBatchService.rejectTransactions(batchId, initiativeId, transactionsRequest);
        Assertions.assertThrows(RuntimeException.class, resultMono::block);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository, never()).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }

    @Test
    void rejectTransactions_ErrorInUpdateBatch(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";

        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.EVALUATING).build();
        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.EVALUATING))
                .thenReturn(Mono.just(rewardBatch));

        //Mock for approved
        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewardBatchId(batchId)
                .rewards(rewardApprovedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",  RewardBatchTrxStatus.REJECTED, transactionsRequest.getReason()))
                .thenReturn(Mono.just(trxMock));

        when(rewardBatchRepository.updateTotals(batchId,1L,  0L,0,0))
                .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        Mono<RewardBatch> resultMono = rewardBatchService.rejectTransactions(batchId, initiativeId, transactionsRequest);
        Assertions.assertThrows(RuntimeException.class, resultMono::block);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(),any(), any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }



    @Test
    void rewardBatchConfirmation_shouldCreateNewBatch_whenSuspendedTransactionsExist() {
        String initiativeId = "INITIATIVE_123";
        String rewardBatchId = "BATCH_123";

        RewardBatch existingBatch = RewardBatch.builder()
                .id(rewardBatchId)
                .status(RewardBatchStatus.EVALUATING)
                .assigneeLevel(RewardBatchAssignee.L2)
                .merchantId("MERCHANT_ID")
                .businessName("MERCHANT_NAME")
                .month("2025-11")
                .name("novembre 2025")
                .numberOfTransactionsSuspended(5L)
                .build();

        RewardBatch approvedBatch = RewardBatch.builder()
                .id(existingBatch.getId())
                .status(RewardBatchStatus.APPROVED)
                .updateDate(LocalDateTime.now())
                .merchantId(existingBatch.getMerchantId())
                .businessName(existingBatch.getBusinessName())
                .month(existingBatch.getMonth())
                .name(existingBatch.getName())
                .numberOfTransactionsSuspended(existingBatch.getNumberOfTransactionsSuspended())
                .build();

        RewardBatch newBatch = RewardBatch.builder()
                .id("NEW_BATCH_ID")
                .merchantId(existingBatch.getMerchantId())
                .month("2025-12")
                .name("dicembre 2025")
                .status(RewardBatchStatus.CREATED)
                .build();

        Mockito.when(rewardBatchRepository.findRewardBatchById(rewardBatchId))
                .thenReturn(Mono.just(existingBatch));

        Mockito.when(rewardBatchRepository.save(Mockito.any(RewardBatch.class)))
                .thenReturn(Mono.just(approvedBatch));

        Mockito.when(rewardBatchRepository.findRewardBatchByFilter(
                        Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(Mono.empty()); // forza la creazione del nuovo batch

        Mockito.when(rewardBatchRepository.save(Mockito.argThat(batch -> batch.getId() == null)))
                .thenReturn(Mono.just(newBatch));

        Mockito.when(rewardTransactionRepository.findByFilter(
                        Mockito.eq(rewardBatchId), Mockito.eq(initiativeId), Mockito.anyList()))
                .thenReturn(Flux.empty());

        StepVerifier.create(rewardBatchService.rewardBatchConfirmation(initiativeId, rewardBatchId))
                .expectNextMatches(batch -> batch.getId().equals("NEW_BATCH_ID")
                        && batch.getStatus().equals(RewardBatchStatus.CREATED))
                .verifyComplete();

        Mockito.verify(rewardBatchRepository, Mockito.times(2)).save(Mockito.any(RewardBatch.class));
    }

    @Test
    void evaluatingRewardBatches(){
        String batchId = "BATCH_ID";
        RewardBatch rewardBatch = RewardBatch.builder()
                .id(batchId)
                .initialAmountCents(100L)
                .build();

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.SENT))
                .thenReturn(Mono.just(rewardBatch));

        Void voidMock = mock(Void.class);
        when(rewardTransactionRepository.rewardTransactionsByBatchId(batchId))
                .thenReturn(Mono.just(voidMock));

        when(rewardBatchRepository.updateStatusAndApprovedAmountCents(batchId,  RewardBatchStatus.EVALUATING, 100L))
                .thenReturn(Mono.just(rewardBatch));

        Long result = rewardBatchService.evaluatingRewardBatches(List.of(batchId)).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1L, result);
    }

    @Test
    void evaluatingRewardBatches_notSent(){
        String batchId = "BATCH_ID";

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.SENT))
                .thenReturn(Mono.empty());

        Mono<Long> monoResult = rewardBatchService.evaluatingRewardBatches(List.of(batchId));
        Assertions.assertThrows(RewardBatchNotFound.class, monoResult::block);

        verify(rewardBatchRepository).findByIdAndStatus(any(), any());
        verify(rewardTransactionRepository, never()).rewardTransactionsByBatchId(any());
        verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), any());
    }

    @Test
    void evaluatingRewardBatches_emptyList(){
        Mono<Long> monoResult = rewardBatchService.evaluatingRewardBatches(new ArrayList<>());
        Assertions.assertThrows(RewardBatchNotFound.class, monoResult::block);

        verify(rewardBatchRepository, never()).findByStatus(any());
        verify(rewardBatchRepository, never()).findByIdAndStatus(any(), any());
        verify(rewardTransactionRepository, never()).rewardTransactionsByBatchId(any());
        verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), any());
    }

    @Test
    void evaluatingRewardBatches_nullList(){
        String batchId = "BATCH_ID_1";
        RewardBatch rewardBatch = RewardBatch.builder()
                .id(batchId)
                .initialAmountCents(100L)
                .build();

        when(rewardBatchRepository.findByStatus(RewardBatchStatus.SENT))
                .thenReturn(Flux.just(rewardBatch));

        Void voidMock = mock(Void.class);
        when(rewardTransactionRepository.rewardTransactionsByBatchId(batchId))
                .thenReturn(Mono.just(voidMock));

        when(rewardBatchRepository.updateStatusAndApprovedAmountCents(batchId,  RewardBatchStatus.EVALUATING, 100L))
                .thenReturn(Mono.just(rewardBatch));

        Long result = rewardBatchService.evaluatingRewardBatches(null).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1L, result);
    }


    @Test
    void evaluatingRewardBatchStatusScheduler() {
        when(rewardBatchRepository.findByStatus(RewardBatchStatus.SENT))
                .thenReturn(Flux.empty());

        rewardBatchServiceSpy.evaluatingRewardBatchStatusScheduler();

        verify(rewardBatchRepository).findByStatus(any());
        verify(rewardBatchRepository, never()).findByIdAndStatus(any(), any());
        verify(rewardTransactionRepository, never()).rewardTransactionsByBatchId(any());
        verify(rewardBatchRepository, never()).updateStatusAndApprovedAmountCents(any(), any(), any());

    }
}
