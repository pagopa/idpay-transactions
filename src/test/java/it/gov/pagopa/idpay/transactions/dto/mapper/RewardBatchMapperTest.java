package it.gov.pagopa.idpay.transactions.dto.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.gov.pagopa.idpay.transactions.dto.RewardBatchDTO;
import it.gov.pagopa.idpay.transactions.enums.PosType;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchAssignee;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDate;
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
    void toDTO_shouldMapAllFieldsAndDefaultSuspendedAmountToZeroWhenNull() {
        LocalDateTime startDate = LocalDateTime.of(2025, 11, 1, 0, 0);
        LocalDateTime endDate = LocalDateTime.of(2025, 11, 30, 23, 59);
        LocalDateTime refundOutcomeTimestamp = LocalDateTime.of(2025, 12, 10, 9, 30);
        LocalDate refundValutaDate = LocalDate.of(2025, 12, 15);
        LocalDateTime merchantSendDate = LocalDateTime.of(2025, 11, 15, 12, 35);

        RewardBatch batch = RewardBatch.builder()
                .id("batch123")
                .merchantId("merchantABC")
                .initiativeId("initiativeXYZ")
                .businessName("Test business")
                .month("2025-11")
                .posType(PosType.PHYSICAL)
                .name("novembre 2025")
                .status(RewardBatchStatus.CREATED)
                .partial(false)
                .startDate(startDate)
                .endDate(endDate)
                .approvedAmountCents(500L)
                .suspendedAmountCents(null)
                .initialAmountCents(1000L)
                .numberOfTransactions(10L)
                .numberOfTransactionsElaborated(8L)
                .numberOfTransactionsSuspended(1L)
                .numberOfTransactionsRejected(1L)
                .reportPath("/reports/report.csv")
                .assigneeLevel(RewardBatchAssignee.L1)
                .refundErrorMessage("refund error")
                .refundOutcomeTimestamp(refundOutcomeTimestamp)
                .refundValutaDate(refundValutaDate)
                .merchantSendDate(merchantSendDate)
                .build();

        Mono<RewardBatchDTO> dtoMono = mapper.toDTO(batch);

        StepVerifier.create(dtoMono)
                .assertNext(dto -> {
                    assertEquals("batch123", dto.getId());
                    assertEquals("merchantABC", dto.getMerchantId());
                    assertEquals("initiativeXYZ", dto.getInitiativeId());
                    assertEquals("Test business", dto.getBusinessName());
                    assertEquals("2025-11", dto.getMonth());
                    assertEquals(PosType.PHYSICAL, dto.getPosType());
                    assertEquals("novembre 2025", dto.getName());
                    assertEquals("CREATED", dto.getStatus());
                    assertFalse(dto.getPartial());
                    assertEquals(startDate, dto.getStartDate());
                    assertEquals(endDate, dto.getEndDate());
                    assertEquals(500L, dto.getApprovedAmountCents());
                    assertEquals(0L, dto.getSuspendedAmountCents());
                    assertEquals(1000L, dto.getInitialAmountCents());
                    assertEquals(10L, dto.getNumberOfTransactions());
                    assertEquals(8L, dto.getNumberOfTransactionsElaborated());
                    assertEquals(1L, dto.getNumberOfTransactionsSuspended());
                    assertEquals(1L, dto.getNumberOfTransactionsRejected());
                    assertEquals("/reports/report.csv", dto.getReportPath());
                    assertEquals("L1", dto.getAssigneeLevel());
                    assertEquals("refund error", dto.getRefundErrorMessage());
                    assertEquals(refundOutcomeTimestamp, dto.getRefundOutcomeTimestamp());
                    assertEquals(refundValutaDate, dto.getRefundValutaDate());
                    assertEquals(merchantSendDate, dto.getMerchantSendDate());
                })
                .verifyComplete();
    }

    @Test
    void toDTO_shouldKeepSuspendedAmountWhenPresentAndMapNullableFields() {
        RewardBatch batch = RewardBatch.builder()
                .id("batch456")
                .merchantId("merchantDEF")
                .initiativeId("initiativeABC")
                .businessName("Another business")
                .month("2025-12")
                .posType(PosType.ONLINE)
                .name("dicembre 2025")
                .status(RewardBatchStatus.APPROVED)
                .partial(true)
                .startDate(null)
                .endDate(null)
                .approvedAmountCents(700L)
                .suspendedAmountCents(300L)
                .initialAmountCents(1200L)
                .numberOfTransactions(20L)
                .numberOfTransactionsElaborated(18L)
                .numberOfTransactionsSuspended(2L)
                .numberOfTransactionsRejected(0L)
                .reportPath(null)
                .assigneeLevel(null)
                .refundErrorMessage(null)
                .refundOutcomeTimestamp(null)
                .refundValutaDate(null)
                .merchantSendDate(null)
                .build();

        Mono<RewardBatchDTO> dtoMono = mapper.toDTO(batch);

        StepVerifier.create(dtoMono)
                .assertNext(dto -> {
                    assertEquals("batch456", dto.getId());
                    assertEquals("merchantDEF", dto.getMerchantId());
                    assertEquals("initiativeABC", dto.getInitiativeId());
                    assertEquals("Another business", dto.getBusinessName());
                    assertEquals("2025-12", dto.getMonth());
                    assertEquals(PosType.ONLINE, dto.getPosType());
                    assertEquals("dicembre 2025", dto.getName());
                    assertEquals("APPROVED", dto.getStatus());
                    assertEquals(true, dto.getPartial());
                    assertNull(dto.getStartDate());
                    assertNull(dto.getEndDate());
                    assertEquals(700L, dto.getApprovedAmountCents());
                    assertEquals(300L, dto.getSuspendedAmountCents());
                    assertEquals(1200L, dto.getInitialAmountCents());
                    assertEquals(20L, dto.getNumberOfTransactions());
                    assertEquals(18L, dto.getNumberOfTransactionsElaborated());
                    assertEquals(2L, dto.getNumberOfTransactionsSuspended());
                    assertEquals(0L, dto.getNumberOfTransactionsRejected());
                    assertNull(dto.getReportPath());
                    assertEquals("null", dto.getAssigneeLevel());
                    assertNull(dto.getRefundErrorMessage());
                    assertNull(dto.getRefundOutcomeTimestamp());
                    assertNull(dto.getRefundValutaDate());
                    assertNull(dto.getMerchantSendDate());
                })
                .verifyComplete();
    }
}