package it.gov.pagopa.idpay.transactions.service;

import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import it.gov.pagopa.idpay.transactions.usecase.rewardbatch.RewardBatchSharedUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RewardBatchServiceImplTest {

    @Mock private RewardBatchRepository rewardBatchRepository;

    private RewardBatchServiceImpl service;

    private static final String MERCHANT_ID = "MERCHANT_ID";
    private static final String BUSINESS_NAME = "Business";
    private static final String BATCH_ID = "BATCH_ID";
    private static final String BATCH_ID_2 = "BATCH_ID_2";

    @BeforeEach
    void setup() {
        service = new RewardBatchServiceImpl(rewardBatchRepository);
    }

    @Test
    void findOrCreateBatch_returnsExisting() {
        RewardBatch existing = RewardBatch.builder()
                .id("EX")
                .merchantId("M1")
                .posType(PHYSICAL)
                .month("2025-11")
                .status(RewardBatchStatus.CREATED)
                .name("novembre 2025")
                .build();

        when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.just(existing));

        StepVerifier.create(service.findOrCreateBatch("M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .expectNext(existing)
                .verifyComplete();

        verify(rewardBatchRepository, never()).save(any());
    }

    @Test
    void findOrCreateBatch_createsNew_whenMissing() {
        when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.empty());

        when(rewardBatchRepository.save(any()))
                .thenAnswer(inv -> {
                    RewardBatch b = inv.getArgument(0);
                    b.setId("NEW");
                    return Mono.just(b);
                });

        StepVerifier.create(service.findOrCreateBatch("M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .assertNext(b -> {
                    assertEquals("NEW", b.getId());
                    assertEquals("M1", b.getMerchantId());
                    assertEquals(PHYSICAL, b.getPosType());
                    assertEquals("2025-11", b.getMonth());
                    assertEquals(RewardBatchStatus.CREATED, b.getStatus());
                    assertTrue(b.getName().contains("novembre"));
                    assertNotNull(b.getStartDate());
                    assertNotNull(b.getEndDate());
                })
                .verifyComplete();

        verify(rewardBatchRepository).save(any());
    }

    @Test
    void findOrCreateBatch_duplicateKey_fallbackFind() {
        RewardBatch existing = RewardBatch.builder()
                .id("DUP")
                .merchantId("M1")
                .posType(PHYSICAL)
                .month("2025-11")
                .status(RewardBatchStatus.CREATED)
                .name("novembre 2025")
                .build();

        when(rewardBatchRepository.findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11"))
                .thenReturn(Mono.empty())
                .thenReturn(Mono.just(existing));

        when(rewardBatchRepository.save(any()))
                .thenReturn(Mono.error(new DuplicateKeyException("dup")));

        StepVerifier.create(service.findOrCreateBatch("M1", PHYSICAL, "2025-11", BUSINESS_NAME))
                .expectNext(existing)
                .verifyComplete();

        verify(rewardBatchRepository).save(any());
        verify(rewardBatchRepository, times(2)).findByMerchantIdAndPosTypeAndMonth("M1", PHYSICAL, "2025-11");
    }

    @Test
    void addOneMonth_and_italian() {
        assertEquals("2026-01", RewardBatchSharedUtils.addOneMonth("2025-12"));
        assertEquals("gennaio 2026", RewardBatchSharedUtils.addOneMonthToItalian("dicembre 2025"));
    }
}