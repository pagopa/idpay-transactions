package it.gov.pagopa.idpay.transactions.dto.mapper;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
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
        .businessName("Test business")
        .month("2025-11")
        .posType(PosType.PHYSICAL)
        .status(RewardBatchStatus.CREATED)
        .partial(false)
        .name("novembre 2025")
        .startDate(LocalDateTime.of(2025, 11, 1, 0, 0))
        .endDate(LocalDateTime.of(2025, 11, 30, 23, 59))
        .merchantSendDate(LocalDateTime.of(2025, 11, 15, 12, 35))
        .initialAmountCents(0L)
        .numberOfTransactions(0L)
        .numberOfTransactionsElaborated(0L)
        .reportPath(null)
        .build();

    Mono<RewardBatchDTO> dtoMono = mapper.toDTO(batch);

    StepVerifier.create(dtoMono)
        .assertNext(dto -> {
          assertEquals("batch123", dto.getId());
          assertEquals("merchantABC", dto.getMerchantId());
          assertEquals("Test business", dto.getBusinessName());
          assertEquals("2025-11", dto.getMonth());
          assertEquals(PosType.PHYSICAL, dto.getPosType());
          assertEquals("CREATED", dto.getStatus());
          assertFalse(dto.getPartial());
          assertEquals("novembre 2025", dto.getName());
          assertEquals(LocalDateTime.of(2025, 11, 1, 0, 0), dto.getStartDate());
          assertEquals(LocalDateTime.of(2025, 11, 30, 23, 59), dto.getEndDate());
          assertEquals(LocalDateTime.of(2025, 11, 15, 12, 35), dto.getMerchantSendDate());
          assertEquals(0L, dto.getInitialAmountCents());
          assertEquals(0L, dto.getNumberOfTransactions());
          assertEquals(0L, dto.getNumberOfTransactionsElaborated());
          assertNull(dto.getReportPath());
        })
        .verifyComplete();
  }
}
