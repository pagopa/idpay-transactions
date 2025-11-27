package it.gov.pagopa.idpay.transactions.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchTrxStatus;
import it.gov.pagopa.idpay.transactions.model.Reward;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.YearMonth;
import java.util.Arrays;
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
  private RewardBatchService rewardBatchService;
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
  void setUp(){
    rewardBatchService = new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionRepository);
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, BUSINESS_NAME))
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

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PosType.PHYSICAL, batchMonth))
        .thenReturn(Mono.just(existingBatch));

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, BUSINESS_NAME))
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, BUSINESS_NAME))
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, BUSINESS_NAME))
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, BUSINESS_NAME))
        .assertNext(batch -> {
          assert batch.getName().equals("novembre 2025");
        })
        .verifyComplete();
  }

  @Test
  void getMerchantRewardBatches_returnsPagedResult() {
    String merchantId = "M1";
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch rb1 = RewardBatch.builder()
        .id("B1")
        .merchantId(merchantId)
        .name("novembre 2025")
        .build();

    RewardBatch rb2 = RewardBatch.builder()
        .id("B2")
        .merchantId(merchantId)
        .name("novembre 2025")
        .build();

    Mockito.when(rewardBatchRepository.findRewardBatchByMerchantId(merchantId, pageable))
        .thenReturn(Flux.just(rb1, rb2));

    Mockito.when(rewardBatchRepository.getCount(merchantId))
        .thenReturn(Mono.just(5L));

    StepVerifier.create(rewardBatchService.getMerchantRewardBatches(merchantId, pageable))
        .assertNext(page -> {
          assert page.getContent().size() == 2;
          assert page.getContent().get(0).getId().equals("B1");
          assert page.getContent().get(1).getId().equals("B2");

          assert page.getTotalElements() == 5;
          assert page.getPageable().equals(pageable);
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).findRewardBatchByMerchantId(merchantId, pageable);
    Mockito.verify(rewardBatchRepository).getCount(merchantId);
  }

  @Test
  void getMerchantRewardBatches_emptyPage() {
    String merchantId = "M1";
    Pageable pageable = PageRequest.of(1, 2);

    when(rewardBatchRepository.findRewardBatchByMerchantId(merchantId, pageable))
        .thenReturn(Flux.empty());

    when(rewardBatchRepository.getCount(merchantId))
        .thenReturn(Mono.just(0L));

    StepVerifier.create(rewardBatchService.getMerchantRewardBatches(merchantId, pageable))
        .assertNext(page -> {
          assert page.getContent().isEmpty();
          assert page.getTotalElements() == 0;
          assert page.getPageable().equals(pageable);
        })
        .verifyComplete();
  }

  @Test
  void getAllRewardBatches_returnsPagedResult() {
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch rb1 = RewardBatch.builder()
        .id("B1")
        .merchantId("MERCHANT1")
        .name("novembre 2025")
        .build();

    RewardBatch rb2 = RewardBatch.builder()
        .id("B2")
        .merchantId("MERCHANT2")
        .name("novembre 2025")
        .build();

    Mockito.when(rewardBatchRepository.findRewardBatch(pageable))
        .thenReturn(Flux.just(rb1, rb2));

    Mockito.when(rewardBatchRepository.getCount())
        .thenReturn(Mono.just(10L));

    StepVerifier.create(rewardBatchService.getAllRewardBatches(pageable))
        .assertNext(page -> {
          assert page.getContent().size() == 2;
          assert page.getTotalElements() == 10;
          assert page.getPageable().equals(pageable);
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).findRewardBatch(pageable);
    Mockito.verify(rewardBatchRepository).getCount();
  }

  @Test
  void getAllRewardBatches_empty() {
    Pageable pageable = PageRequest.of(0, 2);

    Mockito.when(rewardBatchRepository.findRewardBatch(pageable))
        .thenReturn(Flux.empty());

    Mockito.when(rewardBatchRepository.getCount())
        .thenReturn(Mono.just(0L));

    StepVerifier.create(rewardBatchService.getAllRewardBatches(pageable))
        .assertNext(page -> {
          assert page.getContent().isEmpty();
          assert page.getTotalElements() == 0;
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

    Mockito.when(rewardTransactionRepository.rewardTransactionsByBatchId("B1"))
        .thenReturn(Mono.empty());

    StepVerifier.create(rewardBatchService.sendRewardBatch("M1", "B1"))
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).save(any());
    Mockito.verify(rewardTransactionRepository).rewardTransactionsByBatchId("B1");
  }

    @Test
    void suspendTransactions_ok() {
        String batchId = "BATCH1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.CREATED)
                .approvedAmountCents(1000L)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(Arrays.asList("TX1", "TX2"));
        request.setReason("Check");

        String initiativeId = "INIT1";

        when(rewardBatchRepository.findById(batchId))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTransactionsStatus(
                batchId,
                request.getTransactionIds(),
                RewardBatchTrxStatus.SUSPENDED,
                request.getReason()
        )).thenReturn(Mono.just(2L));

        when(rewardTransactionRepository.sumSuspendedAccruedRewardCents(
                batchId,
                request.getTransactionIds(),
                initiativeId
        )).thenReturn(Mono.just(300L));

        when(rewardBatchRepository.updateTotals(
                batchId,
                2L,
                -300L,
                0L,
                2L
        )).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();
    }


    @Test
    void suspendTransactions_noModifiedTransactions() {
        String batchId = "BATCH1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.CREATED)
                .build();

        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(Arrays.asList("TX3", "TX4"));
        request.setReason("Check");

        String initiativeId = "INIT1";

        when(rewardBatchRepository.findById(batchId))
                .thenReturn(Mono.just(batch));

        when(rewardBatchRepository.updateTransactionsStatus(
                batchId,
                request.getTransactionIds(),
                RewardBatchTrxStatus.SUSPENDED,
                request.getReason()
        )).thenReturn(Mono.just(0L));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectNext(batch)
                .verifyComplete();

        verify(rewardTransactionRepository, never()).sumSuspendedAccruedRewardCents(any(), any(), any());
        verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void suspendTransactions_suspendedTotalZero_returnsOriginalBatch() {
        String batchId = "batch123";
        String initiativeId = "init123";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(List.of("trx1", "trx2"));
        request.setReason("Test reason");

        RewardBatch batch = new RewardBatch();
        batch.setId(batchId);
        batch.setStatus(RewardBatchStatus.CREATED);

        when(rewardBatchRepository.findById(batchId)).thenReturn(Mono.just(batch));
        when(rewardBatchRepository.updateTransactionsStatus(
                eq(batchId),
                anyList(),
                eq(RewardBatchTrxStatus.SUSPENDED),
                anyString()
        )).thenReturn(Mono.just(2L));

        when(rewardTransactionRepository.sumSuspendedAccruedRewardCents(
                batchId,
                request.getTransactionIds(),
                initiativeId
        )).thenReturn(Mono.just(0L));

        Mono<RewardBatch> result = rewardBatchService.suspendTransactions(batchId, initiativeId, request);

        StepVerifier.create(result)
                .expectNext(batch)
                .verifyComplete();

        verify(rewardBatchRepository, never()).updateTotals(anyString(), anyInt(), anyLong(), anyInt(), anyInt());
    }

    @Test
    void suspendTransactions_throwsException_whenBatchApproved_serviceLevel() {
        String batchId = "BATCH_APPROVED";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(Collections.singletonList("trx1"));
        request.setReason("Test reason");

        String initiativeId = "INIT1";

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.APPROVED)
                .build();

        when(rewardBatchRepository.findById(batchId)).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, initiativeId, request))
                .expectErrorMatches(throwable ->
                        throwable instanceof IllegalStateException &&
                                throwable.getMessage().equals("Cannot suspend transactions on an APPROVED batch"))
                .verify();

        verify(rewardBatchRepository, never()).updateTransactionsStatus(any(), any(), any(), any());
        verify(rewardTransactionRepository, never()).sumSuspendedAccruedRewardCents(any(), any(), any());
        verify(rewardBatchRepository, never()).updateTotals(any(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void rewardBatchConfirmation_Success_WithSuspendedTransactions() {
        REWARD_BATCH_OLD.setMonth(YearMonth.of(2025, 11).toString());
        REWARD_BATCH_OLD.setName("novembre 2025");
        REWARD_BATCH_OLD.setNumberOfTransactionsSuspended(10L);

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
                        throwable instanceof RewardBatchException &&
                                ((RewardBatchException) throwable).getHttpStatus().equals(HttpStatus.NOT_FOUND)
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
                        throwable instanceof RewardBatchException &&
                                ((RewardBatchException) throwable).getHttpStatus().equals(HttpStatus.BAD_REQUEST)
                )
                .verify();

        verify(rewardBatchRepository, times(0)).save(any());
        verify(rewardBatchServiceSpy, times(0)).createRewardBatchAndSave(any());
    }

    @Test
    void approvedTransactions () {
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";
        String merchantId = "MERCHANT_ID";

        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxApproved", "trxToCheck", "trxConsultable", "trxSuspended", "trxRejected")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.APPROVED).build();

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.APPROVED))
                .thenReturn(Mono.just(rewardBatch));

        //Mock for approved
        Reward rewardApproved = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, rewardApproved);
        RewardTransaction trxApprovedMock = RewardTransaction.builder()
                .id("trxApproved")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.APPROVED)
                .rewards(rewardApprovedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxApproved",  RewardBatchTrxStatus.APPROVED))
                .thenReturn(Mono.just(trxApprovedMock));

        //Mock for to_check
        Reward rewardToCheck = Reward.builder().accruedRewardCents(2000L).build();
        Map<String, Reward> rewardToCheckMap = Map.of(initiativeId, rewardToCheck);
        RewardTransaction trxToCheckMock = RewardTransaction.builder()
                .id("trxToCheck")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(rewardToCheckMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxToCheck",  RewardBatchTrxStatus.APPROVED))
                .thenReturn(Mono.just(trxToCheckMock));

        //Mock for consultable
        Reward rewardConsultable = Reward.builder().accruedRewardCents(2500L).build();
        Map<String, Reward> rewardConsultableMap = Map.of(initiativeId, rewardConsultable);
        RewardTransaction trxConsultableMock = RewardTransaction.builder()
                .id("trxConsultable")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.CONSULTABLE)
                .rewards(rewardConsultableMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxConsultable",  RewardBatchTrxStatus.APPROVED))
                .thenReturn(Mono.just(trxConsultableMock));

        //Mock for suspended
        Reward rewardSuspended = Reward.builder().accruedRewardCents(3000L).build();
        Map<String, Reward> rewardSuspendedMap = Map.of(initiativeId, rewardSuspended);
        RewardTransaction trxSuspendedMock = RewardTransaction.builder()
                .id("trxSuspended")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.SUSPENDED)
                .rewards(rewardSuspendedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxSuspended",  RewardBatchTrxStatus.APPROVED))
                .thenReturn(Mono.just(trxSuspendedMock));

        //Mock for rejected
        Reward rewardRejected = Reward.builder().accruedRewardCents(3500L).build();
        Map<String, Reward> rewardRejectedMap = Map.of(initiativeId, rewardRejected);
        RewardTransaction trxRejectedMock = RewardTransaction.builder()
                .id("trxRejected")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.REJECTED)
                .rewards(rewardRejectedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxRejected",  RewardBatchTrxStatus.APPROVED))
                .thenReturn(Mono.just(trxRejectedMock));

        RewardBatch expectedResult = new RewardBatch();
        when(rewardBatchRepository.updateTotals(
                batchId,
                2L,
                rewardSuspended.getAccruedRewardCents()+rewardRejected.getAccruedRewardCents(),
                -1L,
                -1L))
                .thenReturn(Mono.just(expectedResult));


        RewardBatch result = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId, merchantId).block();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedResult, result);
        verify(rewardTransactionRepository, times(5)).updateStatusAndReturnOld(any(), any(),any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }

    @Test
    void approvedTransactions_NotFoundBatch(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";
        String merchantId = "MERCHANT_ID";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.APPROVED))
                .thenReturn(Mono.empty());

        Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId, merchantId);
        Assertions.assertThrows(IllegalArgumentException.class, resultMono::block);

        verify(rewardTransactionRepository, never()).updateStatusAndReturnOld(any(), any(),any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository, never()).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());

    }

    @Test
    void approvedTransactions_ErrorInUpdateInModifyTrx(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";
        String merchantId = "MERCHANT_ID";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.APPROVED).build();
        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.APPROVED))
                .thenReturn(Mono.just(rewardBatch));

        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",  RewardBatchTrxStatus.APPROVED))
                .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId, merchantId);
        Assertions.assertThrows(RuntimeException.class, resultMono::block);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(),any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository, never()).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }

    @Test
    void approvedTransactions_ErrorInUpdateBatch(){
        String batchId = "BATCH_ID";
        String initiativeId = "INITIATIVE_ID";
        String merchantId = "MERCHANT_ID";
        TransactionsRequest transactionsRequest = TransactionsRequest.builder()
                .transactionIds(List.of("trxId")).build();

        RewardBatch rewardBatch = RewardBatch.builder().id(batchId).status(RewardBatchStatus.APPROVED).build();
        when(rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.APPROVED))
                .thenReturn(Mono.just(rewardBatch));

        //Mock for approved
        Reward reward = Reward.builder().accruedRewardCents(1000L).build();
        Map<String, Reward> rewardApprovedMap = Map.of(initiativeId, reward);
        RewardTransaction trxMock = RewardTransaction.builder()
                .id("trxId")
                .rewardBatchTrxStatus(RewardBatchTrxStatus.TO_CHECK)
                .rewards(rewardApprovedMap).build();
        when(rewardTransactionRepository.updateStatusAndReturnOld(batchId, "trxId",  RewardBatchTrxStatus.APPROVED))
                .thenReturn(Mono.just(trxMock));

        when(rewardBatchRepository.updateTotals(batchId,1L,  0L,0,0))
                .thenReturn(Mono.error(new RuntimeException("DUMMY_EXCEPTION")));

        Mono<RewardBatch> resultMono = rewardBatchService.approvedTransactions(batchId, transactionsRequest, initiativeId, merchantId);
        Assertions.assertThrows(RuntimeException.class, resultMono::block);

        verify(rewardTransactionRepository).updateStatusAndReturnOld(any(), any(),any());
        verify(rewardBatchRepository).findByIdAndStatus(any(),any());
        verify(rewardBatchRepository).updateTotals(any(),anyLong(),anyLong(), anyLong(), anyLong());
    }
}
