package it.gov.pagopa.idpay.transactions.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.dto.RewardBatchListDTO;
import it.gov.pagopa.idpay.transactions.dto.mapper.RewardBatchMapper;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.service.RewardBatchService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;

@WebFluxTest(controllers = MerchantRewardBatchControllerImpl.class)
class MerchantRewardBatchControllerImplTest {

  @Autowired
  protected WebTestClient webClient;

  @MockitoBean
  RewardBatchService rewardBatchService;

  @MockitoBean
  RewardBatchMapper rewardBatchMapper;

  private static final String MERCHANT_ID = "MERCHANT_ID";
  private static final String INITIATIVE_ID = "INIT1";

  @Test
  void getMerchantRewardBatchesOk() {
    RewardBatch batch = RewardBatch.builder()
        .id("BATCH1")
        .name("Reward Batch 1")
        .build();

    Page<RewardBatch> page = new PageImpl<>(
        List.of(batch),
        PageRequest.of(0, 10),
        1
    );

    RewardBatchDTO dto = RewardBatchDTO.builder()
        .id(batch.getId())
        .name(batch.getName())
        .build();

    when(rewardBatchService.getMerchantRewardBatches(
        eq(MERCHANT_ID),
        isNull(),
        isNull(),
        any(Pageable.class)))
        .thenReturn(Mono.just(page));

    when(rewardBatchMapper.toDTO(batch))
        .thenReturn(Mono.just(dto));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches")
            .queryParam("page", 0)
            .queryParam("size", 10)
            .build(INITIATIVE_ID))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isOk()
        .expectBody(RewardBatchListDTO.class)
        .value(res -> {
          assertNotNull(res);
          assertEquals(1, res.getContent().size());
          assertEquals("BATCH1", res.getContent().getFirst().getId());
          assertEquals("Reward Batch 1", res.getContent().getFirst().getName());
          assertEquals(1, res.getTotalElements());
          assertEquals(1, res.getTotalPages());
          assertEquals(10, res.getPageSize());
        });

    verify(rewardBatchService, times(1)).getMerchantRewardBatches(
        eq(MERCHANT_ID),
        isNull(),
        isNull(),
        any(Pageable.class));
    verify(rewardBatchMapper, times(1)).toDTO(batch);
  }

  @Test
  void getRewardBatchesOk() {
    RewardBatch batch = RewardBatch.builder()
        .id("BATCH1")
        .name("Reward Batch 1")
        .build();

    Page<RewardBatch> page = new PageImpl<>(
        List.of(batch),
        PageRequest.of(0, 10),
        1
    );

    RewardBatchDTO dto = RewardBatchDTO.builder()
        .id(batch.getId())
        .name(batch.getName())
        .build();

    when(rewardBatchService.getAllRewardBatches(
        isNull(),
        isNull(),
        any(Pageable.class)))
        .thenReturn(Mono.just(page));

    when(rewardBatchMapper.toDTO(batch))
        .thenReturn(Mono.just(dto));

    webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches")
            .queryParam("page", 0)
            .queryParam("size", 10)
            .build(INITIATIVE_ID))
        .exchange()
        .expectStatus().isOk()
        .expectBody(RewardBatchListDTO.class)
        .value(res -> {
          assertNotNull(res);
          assertEquals(1, res.getContent().size());
          assertEquals("BATCH1", res.getContent().getFirst().getId());
          assertEquals("Reward Batch 1", res.getContent().getFirst().getName());
          assertEquals(1, res.getTotalElements());
          assertEquals(1, res.getTotalPages());
          assertEquals(10, res.getPageSize());
        });

    verify(rewardBatchService, times(1)).getAllRewardBatches(
        isNull(),
        isNull(),
        any(Pageable.class));
    verify(rewardBatchMapper, times(1)).toDTO(batch);
  }

  @Test
  void sendRewardBatchesOk() {

    String batchId = "BATCH1";

    when(rewardBatchService.sendRewardBatch(MERCHANT_ID, batchId))
        .thenReturn(Mono.empty());

    webClient.post()
        .uri(uriBuilder -> uriBuilder
            .path("/idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{batchId}/send")
            .build(INITIATIVE_ID, batchId))
        .header("x-merchant-id", MERCHANT_ID)
        .exchange()
        .expectStatus().isNoContent();

    verify(rewardBatchService, times(1))
        .sendRewardBatch(MERCHANT_ID, batchId);
  }
}
