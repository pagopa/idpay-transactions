package it.gov.pagopa.idpay.transactions.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;

import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.YearMonth;

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

  private static final String BUSINESS_NAME = "Test Business name";

  @BeforeEach
  void setUp() {
    rewardBatchService = new RewardBatchServiceImpl(rewardBatchRepository,
        rewardTransactionRepository);
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
  void getMerchantRewardBatches_returnsPagedResult() {
    String merchantId = "M1";
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch rb1 = RewardBatch.builder().id("B1").merchantId(merchantId).name("novembre 2025")
        .build();
    RewardBatch rb2 = RewardBatch.builder().id("B2").merchantId(merchantId).name("novembre 2025")
        .build();

    Mockito.when(
            rewardBatchRepository.findRewardBatchByMerchantId(merchantId, status, assigneeLevel,
                pageable))
        .thenReturn(Flux.just(rb1, rb2));
    Mockito.when(rewardBatchRepository.getCount(merchantId, status, assigneeLevel))
        .thenReturn(Mono.just(5L));

    StepVerifier.create(
            rewardBatchService.getMerchantRewardBatches(merchantId, status, assigneeLevel, pageable)
        )
        .assertNext(page -> {
          assert page.getContent().size() == 2;
          assert page.getContent().get(0).getId().equals("B1");
          assert page.getContent().get(1).getId().equals("B2");
          assert page.getTotalElements() == 5;
          assert page.getPageable().equals(pageable);
        })
        .verifyComplete();
  }

  @Test
  void getMerchantRewardBatches_emptyPage() {
    String merchantId = "M1";
    String status = null;
    String assigneeLevel = null;
    Pageable pageable = PageRequest.of(1, 2);

    Mockito.when(rewardBatchRepository.findRewardBatchByMerchantId(
            merchantId,
            status,
            assigneeLevel,
            pageable))
        .thenReturn(Flux.empty());

    Mockito.when(rewardBatchRepository.getCount(merchantId, status, assigneeLevel))
        .thenReturn(Mono.just(0L));

    StepVerifier.create(
            rewardBatchService.getMerchantRewardBatches(merchantId, status, assigneeLevel, pageable)
        )
        .assertNext(page -> {
          assert page.getContent().isEmpty();
          assert page.getTotalElements() == 0;
          assert page.getPageable().equals(pageable);
        })
        .verifyComplete();
  }

  @Test
  void getAllRewardBatches_returnsPagedResult() {
    String status = null;
    String assigneeLevel = null;
    String organizationRole = null;
    Pageable pageable = PageRequest.of(0, 2);

    RewardBatch batchA = RewardBatch.builder()
        .id("B1")
        .merchantId("MERCHANT1")
        .name("novembre 2025")
        .build();

    RewardBatch batchB = RewardBatch.builder()
        .id("B2")
        .merchantId("MERCHANT2")
        .name("novembre 2025")
        .build();

    Mockito.when(rewardBatchRepository.findRewardBatch(status, assigneeLevel, false, pageable))
        .thenReturn(Flux.just(batchA, batchB));

    Mockito.when(rewardBatchRepository.getCount(status, assigneeLevel, false))
        .thenReturn(Mono.just(10L));

    StepVerifier.create(
            rewardBatchService.getAllRewardBatches(status, assigneeLevel, organizationRole, pageable)
        )
        .assertNext(page -> {
          assert page.getContent().size() == 2;
          assert page.getTotalElements() == 10;
          assert page.getPageable().equals(pageable);
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).findRewardBatch(status, assigneeLevel, false, pageable);
    Mockito.verify(rewardBatchRepository).getCount(status, assigneeLevel, false);
  }

  @Test
  void getAllRewardBatches_empty() {
    String status = null;
    String assigneeLevel = null;
    String organizationRole = null;
    Pageable pageable = PageRequest.of(0, 2);

    Mockito.when(rewardBatchRepository.findRewardBatch(status, assigneeLevel, false, pageable))
        .thenReturn(Flux.empty());

    Mockito.when(rewardBatchRepository.getCount(status, assigneeLevel, false))
        .thenReturn(Mono.just(0L));

    StepVerifier.create(
            rewardBatchService.getAllRewardBatches(status, assigneeLevel, organizationRole, pageable)
        )
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
}



