package it.gov.pagopa.idpay.transactions.dto.mapper;

import static org.junit.jupiter.api.Assertions.*;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;

import java.time.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RewardBatchMapperTest {

    private RewardBatchMapper mapper;
    private ZoneId zone;

    @BeforeEach
    void setUp() {
        mapper = new RewardBatchMapper();
        zone = ZoneId.systemDefault();
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
                .startDate(LocalDate.of(2025, 11, 1)
                        .atStartOfDay(zone)
                        .toInstant())
                .endDate(LocalDate.of(2025, 11, 30)
                        .atTime(23, 59)
                        .atZone(zone)
                        .toInstant())
                .merchantSendDate(LocalDate.of(2025, 11, 15)
                        .atTime( 12, 35)
                        .atZone(zone)
                        .toInstant())
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

                    assertEquals(
                            LocalDate.of(2025, 11, 1)
                                    .atStartOfDay(zone)
                                    .toInstant(),
                            dto.getStartDate()
                    );

                    assertEquals(
                            LocalDate.of(2025, 11, 30)
                                    .atTime(23, 59)
                                    .atZone(zone)
                                    .toInstant(),
                            dto.getEndDate()
                    );

                    assertEquals(
                            LocalDate.of(2025, 11, 15)
                                    .atTime(12, 35)
                                    .atZone(zone)
                                    .toInstant(),
                            dto.getMerchantSendDate()
                    );

                    assertEquals(0L, dto.getInitialAmountCents());
                    assertEquals(0L, dto.getNumberOfTransactions());
                    assertEquals(0L, dto.getNumberOfTransactionsElaborated());
                    assertNull(dto.getReportPath());
                })
                .verifyComplete();
    }
}