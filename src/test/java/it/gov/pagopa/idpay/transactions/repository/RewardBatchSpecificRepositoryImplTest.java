package it.gov.pagopa.idpay.transactions.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.common.reactive.mongo.MongoTest;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@DirtiesContext
@MongoTest
class RewardBatchSpecificRepositoryImplTest {

  @Autowired
  protected RewardBatchRepository rewardBatchRepository;

  @Autowired
  private RewardBatchSpecificRepositoryImpl rewardBatchSpecificRepository;

  private RewardBatch batch1;
  private RewardBatch batch2;

//  @BeforeEach
//  void setUp() {
//    rewardBatchRepository.deleteAll().block();
//
//    batch1 = RewardBatch.builder()
//        .id("batch1")
//        .merchantId("merchantA")
//        .month("2025-11")
//        .posType(PosType.PHYSICAL)
//        .batchType(BatchType.REGULAR)
//        .status(RewardBatchStatus.CREATED)
//        .partial(true)
//        .name("novembre 2025 - fisico")
//        .startDate(LocalDateTime.of(2025, 11, 1, 0, 0))
//        .endDate(LocalDateTime.of(2025, 11, 30, 23, 59))
//        .totalAmountCents(1000L)
//        .build();
//
//    batch2 = RewardBatch.builder()
//        .id("batch2")
//        .merchantId("merchantA")
//        .month("2025-11")
//        .posType(PosType.ONLINE)
//        .batchType(BatchType.REJECTED)
//        .status(RewardBatchStatus.APPROVED)
//        .partial(false)
//        .name("novembre 2025 - online")
//        .startDate(LocalDateTime.of(2025, 11, 1, 0, 0))
//        .endDate(LocalDateTime.of(2025, 11, 30, 23, 59))
//        .totalAmountCents(2000L)
//        .build();
//
//    rewardBatchRepository.saveAll(Flux.just(batch1, batch2)).blockLast();
//  }
//
//  @AfterEach
//  void clearData() {
//    rewardBatchRepository.deleteAll().block();
//  }
//
//  @Test
//  void findRewardBatchByMerchantId_shouldReturnAllBatches() {
//    Pageable pageable = PageRequest.of(0, 10);
//    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByMerchantId("merchantA", pageable);
//
//    List<RewardBatch> list = result.toStream().toList();
//    assertEquals(2, list.size());
//    assertTrue(list.contains(batch1));
//    assertTrue(list.contains(batch2));
//  }
//
//  @Test
//  void getCount_shouldReturnCorrectNumber() {
//    Mono<Long> countMono = rewardBatchSpecificRepository.getCount("merchantA");
//    Long count = countMono.block();
//    assertEquals(2L, count);
//  }
//
//  @Test
//  void findRewardBatchByMerchantId_withDefaultPageable_shouldReturnSortedBatches() {
//    Flux<RewardBatch> result = rewardBatchSpecificRepository.findRewardBatchByMerchantId("merchantA", null);
//    List<RewardBatch> list = result.toStream().toList();
//    assertEquals(2, list.size());
//    assertEquals("novembre 2025 - fisico", list.get(0).getName());
//    assertEquals("novembre 2025 - online", list.get(1).getName());
//  }
//
//  @Test
//  void findRewardBatchByMerchantId_withPagination_shouldRespectPageSize() {
//    Pageable firstPage = PageRequest.of(0, 1, Sort.by("id").ascending());
//    List<RewardBatch> page1 = rewardBatchSpecificRepository
//        .findRewardBatchByMerchantId("merchantA", firstPage)
//        .toStream().toList();
//
//    assertEquals(1, page1.size());
//    assertEquals("batch1", page1.get(0).getId());
//
//    Pageable secondPage = PageRequest.of(1, 1, Sort.by("id").ascending());
//    List<RewardBatch> page2 = rewardBatchSpecificRepository
//        .findRewardBatchByMerchantId("merchantA", secondPage)
//        .toStream().toList();
//
//    assertEquals(1, page2.size());
//    assertEquals("batch2", page2.get(0).getId());
//
//    assertNotEquals(page1.get(0).getId(), page2.get(0).getId());
//  }
}
