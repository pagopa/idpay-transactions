package it.gov.pagopa.idpay.transactions.service;

import static org.mockito.ArgumentMatchers.any;

import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.YearMonth;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RewardBatchServiceImplTest {

  @Mock
  private RewardBatchRepository rewardBatchRepository;

  private RewardBatchService rewardBatchService;

  @BeforeEach
  void setUp(){
    rewardBatchService = new RewardBatchServiceImpl(rewardBatchRepository);
  }


  @Test
  void findOrCreateBatch_createsNewBatch() {
    YearMonth month = YearMonth.of(2025, 11);

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonthAndBatchType(
            "M1", PosType.PHYSICAL, month, null))
        .thenReturn(Mono.empty());

    Mockito.when(rewardBatchRepository.save(any()))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, month, null))
        .assertNext(batch -> {
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == PosType.PHYSICAL;
          assert batch.getBatchType() == null;
          assert batch.getStatus() == RewardBatchStatus.CREATED;
          assert batch.getName().contains("novembre 2025");
          assert !batch.getName().contains("accettati");
          assert !batch.getName().contains("rigettati");
          assert batch.getStartDate().equals(month.atDay(1).atStartOfDay());
          assert batch.getEndDate().equals(month.atEndOfMonth().atTime(23, 59, 59));
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository).save(any());
  }


  @Test
  void findOrCreateBatch_existingBatch() {
    YearMonth month = YearMonth.of(2025, 11);

    RewardBatch existingBatch = RewardBatch.builder()
        .id("BATCH1")
        .merchantId("M1")
        .posType(PosType.PHYSICAL)
        .month(month)
        .batchType(null)
        .status(RewardBatchStatus.CREATED)
        .name("novembre 2025 - fisico")
        .build();

    Mockito.when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonthAndBatchType("M1", PosType.PHYSICAL, month, null))
        .thenReturn(Mono.just(existingBatch));

    StepVerifier.create(rewardBatchService.findOrCreateBatch("M1", PosType.PHYSICAL, month, null))
        .assertNext(batch -> {
          assert batch.getMerchantId().equals("M1");
          assert batch.getPosType() == PosType.PHYSICAL;
          assert batch.getBatchType() == null;
          assert batch.getStatus() == RewardBatchStatus.CREATED;
          assert batch.getName().contains("novembre 2025");
          assert !batch.getName().contains("accettati");
          assert !batch.getName().contains("rigettati");
        })
        .verifyComplete();

    Mockito.verify(rewardBatchRepository, Mockito.never()).save(any());
  }
}
