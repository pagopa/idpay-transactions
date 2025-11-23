package it.gov.pagopa.idpay.transactions.service;

import static org.mockito.ArgumentMatchers.any;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.YearMonth;

import it.gov.pagopa.idpay.transactions.repository.RewardTransactionSpecificRepository;
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
  private RewardTransactionSpecificRepository rewardTransactionSpecificRepository;

  private RewardBatchService rewardBatchService;

  private static String businessName = "Test Business name";

  @BeforeEach
  void setUp(){
    rewardBatchService = new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionSpecificRepository);
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, businessName))
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, businessName))
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
            new RewardBatchServiceImpl(rewardBatchRepository, rewardTransactionSpecificRepository)
                .findOrCreateBatch("M1", posType, batchMonth, businessName)
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, batchMonth, businessName))
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, businessName))
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

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.ONLINE, batchMonth, businessName))
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

    Mockito.when(rewardBatchRepository.findRewardBatchByMerchantId(merchantId, pageable))
        .thenReturn(Flux.empty());

    Mockito.when(rewardBatchRepository.getCount(merchantId))
        .thenReturn(Mono.just(0L));

    StepVerifier.create(rewardBatchService.getMerchantRewardBatches(merchantId, pageable))
        .assertNext(page -> {
          assert page.getContent().isEmpty();
          assert page.getTotalElements() == 0;
          assert page.getPageable().equals(pageable);
        })
        .verifyComplete();
  }
}
