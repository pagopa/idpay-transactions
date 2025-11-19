package it.gov.pagopa.idpay.transactions.dto.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.enums.BatchType;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RewardBatchMapperTest {

  private RewardBatchMapper mapper;

  @BeforeEach
  void setUp() {
    mapper = new RewardBatchMapper();
  }

  @Test
  void toDTO() {
    RewardBatch batch = RewardBatch.builder()
        .id("batch123")
        .merchantId("merchantABC")
        .month("2025-11")
        .posType(PosType.PHYSICAL)
        .batchType(BatchType.REGULAR)
        .status(RewardBatchStatus.CREATED)
        .partial(true)
        .name("Test Batch")
        .startDate(LocalDateTime.of(2025, 11, 1, 0, 0))
        .endDate(LocalDateTime.of(2025, 11, 30, 23, 59))
        .totalAmountCents(10000L)
        .build();

    Mono<RewardBatchDTO> dtoMono = mapper.toDTO(batch);

    StepVerifier.create(dtoMono)
        .assertNext(dto -> {
          assertEquals("batch123", dto.getId());
          assertEquals("merchantABC", dto.getMerchantId());
          assertEquals("2025-11", dto.getMonth());
          assertEquals("PHYSICAL", dto.getPosType());
          assertEquals("REGULAR", dto.getBatchType());
          assertEquals("CREATED", dto.getStatus());
          assertTrue(dto.getPartial());
          assertEquals("Test Batch", dto.getName());
          assertEquals(LocalDateTime.of(2025, 11, 1, 0, 0), dto.getStartDate());
          assertEquals(LocalDateTime.of(2025, 11, 30, 23, 59), dto.getEndDate());
          assertEquals(10000L, dto.getTotalAmountCents());
        })
        .verifyComplete();
  }
}
