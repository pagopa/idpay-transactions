package it.gov.pagopa.idpay.transactions.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import it.gov.pagopa.common.web.exception.RewardBatchException;
import com.mongodb.client.result.UpdateResult;
import it.gov.pagopa.idpay.transactions.dto.TransactionsRequest;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.model.RewardTransaction;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.Collections;

import it.gov.pagopa.idpay.transactions.repository.RewardTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
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
    @Mock
  private ReactiveMongoTemplate reactiveMongoTemplate;

    

  @BeforeEach
  void setUp(){
    rewardBatchService = new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionRepository, reactiveMongoTemplate);
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
            new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionRepository, reactiveMongoTemplate)
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
    void suspendTransactions_updatesBatchCorrectly_whenTransactionsExist() {
        String batchId = "BATCH1";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(Arrays.asList("trx1", "trx2"));
        request.setReason("Test reason");

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.CREATED)
                .numberOfTransactions(5L)
                .numberOfTransactionsElaborated(0L)
                .approvedAmountCents(3000L)
                .numberOfTransactionsSuspended(0L)
                .build();

        when(rewardBatchRepository.findById(batchId)).thenReturn(Mono.just(batch));

        when(reactiveMongoTemplate.updateMulti(
                any(Query.class),
                any(Update.class),
                eq(RewardTransaction.class))
        ).thenReturn(Mono.just(UpdateResult.acknowledged(2, 2L, null)));

        RewardBatchServiceImpl.TotalAmount totalAmount = new RewardBatchServiceImpl.TotalAmount();
        totalAmount.setTotal(2500L);

        when(reactiveMongoTemplate.aggregate(any(), eq(RewardTransaction.class), eq(RewardBatchServiceImpl.TotalAmount.class)))
                .thenReturn(Flux.just(totalAmount));

        RewardBatch updatedBatch = RewardBatch.builder()
                .id(batchId)
                .numberOfTransactions(3L)
                .numberOfTransactionsElaborated(2L)
                .approvedAmountCents(500L)
                .numberOfTransactionsSuspended(2L)
                .build();

        when(reactiveMongoTemplate.findAndModify(any(Query.class), any(Update.class),
                any(FindAndModifyOptions.class), eq(RewardBatch.class)))
                .thenReturn(Mono.just(updatedBatch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, request))
                .assertNext(b -> {
                    assert b.getNumberOfTransactions() == 3L;
                    assert b.getNumberOfTransactionsElaborated() == 2L;
                    assert b.getApprovedAmountCents() == 500L;
                    assert b.getNumberOfTransactionsSuspended() == 2L;
                })
                .verifyComplete();
    }

    @Test
    void suspendTransactions_returnsBatchUnchanged_whenNoTransactionsModified() {
        String batchId = "BATCH2";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(Collections.singletonList("trx1"));
        request.setReason("Test reason");

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.CREATED)
                .numberOfTransactions(3L)
                .numberOfTransactionsElaborated(1L)
                .approvedAmountCents(1000L)
                .numberOfTransactionsSuspended(1L)
                .build();

        when(rewardBatchRepository.findById(batchId)).thenReturn(Mono.just(batch));

        when(reactiveMongoTemplate.updateMulti(
                any(Query.class),
                any(Update.class),
                eq(RewardTransaction.class))
        ).thenReturn(Mono.just(UpdateResult.acknowledged(0, 0L, null)));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, request))
                .assertNext(b -> {
                    assert b.getNumberOfTransactions() == 3L;
                    assert b.getNumberOfTransactionsElaborated() == 1L;
                    assert b.getApprovedAmountCents() == 1000L;
                    assert b.getNumberOfTransactionsSuspended() == 1L;
                })
                .verifyComplete();

        verify(reactiveMongoTemplate, never())
                .findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class), eq(RewardBatch.class));
    }

    @Test
    void suspendTransactions_throwsException_whenBatchApproved() {
        String batchId = "BATCH_APPROVED";
        TransactionsRequest request = new TransactionsRequest();
        request.setTransactionIds(Collections.singletonList("trx1"));
        request.setReason("Test reason");

        RewardBatch batch = RewardBatch.builder()
                .id(batchId)
                .status(RewardBatchStatus.APPROVED)
                .build();

        when(rewardBatchRepository.findById(batchId)).thenReturn(Mono.just(batch));

        StepVerifier.create(rewardBatchService.suspendTransactions(batchId, request))
                .expectErrorMatches(throwable -> throwable instanceof IllegalStateException &&
                        throwable.getMessage().equals("Cannot suspend transactions on an APPROVED batch"))
                .verify();

        verify(reactiveMongoTemplate, never())
                .updateMulti(any(), any(), eq(RewardTransaction.class));
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
}
