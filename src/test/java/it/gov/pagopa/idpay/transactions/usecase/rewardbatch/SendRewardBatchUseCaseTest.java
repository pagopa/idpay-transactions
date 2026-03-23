package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.exception.RewardBatchException;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.YearMonth;

import static it.gov.pagopa.idpay.transactions.enums.PosType.PHYSICAL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SendRewardBatchUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    private SendRewardBatchUseCase useCase;
    private static final String MERCHANT_ID = "MERCHANT_ID";
    private static final String BATCH_ID = "BATCH_ID";

    @BeforeEach
    void setup() { useCase = new SendRewardBatchUseCase(rewardBatchRepository); }

    @Test
    void execute_batchNotFound() {
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.empty());
        StepVerifier.create(useCase.execute(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class).verify();
    }

    @Test
    void execute_merchantMismatch() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId("OTHER").status(RewardBatchStatus.CREATED).month("2025-01").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        StepVerifier.create(useCase.execute(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class).verify();
    }

    @Test
    void execute_invalidStatus() {
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.SENT).month("2025-01").posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        StepVerifier.create(useCase.execute(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class).verify();
    }

    @Test
    void execute_monthTooEarly() {
        YearMonth now = YearMonth.now();
        RewardBatch b = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.CREATED).month(now.toString()).posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(b));
        StepVerifier.create(useCase.execute(MERCHANT_ID, BATCH_ID))
                .expectErrorSatisfies(ex -> {
                    assertInstanceOf(RewardBatchException.class, ex);
                    assertTrue(ex.getMessage().contains("REWARD_BATCH_MONTH_TOO_EARLY"));
                }).verify();
    }

    @Test
    void execute_previousNotSent() {
        YearMonth batchMonth = YearMonth.now().minusMonths(1);
        RewardBatch current = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.CREATED).month(batchMonth.toString()).posType(PHYSICAL).build();
        RewardBatch previousCreated = RewardBatch.builder().id("PREV").merchantId(MERCHANT_ID).status(RewardBatchStatus.CREATED).month(batchMonth.minusMonths(1).toString()).posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchRepository.findByMerchantIdAndPosType(MERCHANT_ID, PHYSICAL)).thenReturn(Flux.just(previousCreated));
        StepVerifier.create(useCase.execute(MERCHANT_ID, BATCH_ID))
                .expectError(RewardBatchException.class).verify();
    }

    @Test
    void execute_success_allPreviousSent() {
        YearMonth batchMonth = YearMonth.now().minusMonths(1);
        RewardBatch current = RewardBatch.builder().id(BATCH_ID).merchantId(MERCHANT_ID).status(RewardBatchStatus.CREATED).month(batchMonth.toString()).posType(PHYSICAL).build();
        RewardBatch previousSent = RewardBatch.builder().id("PREV").merchantId(MERCHANT_ID).status(RewardBatchStatus.SENT).month(batchMonth.minusMonths(1).toString()).posType(PHYSICAL).build();
        when(rewardBatchRepository.findById(BATCH_ID)).thenReturn(Mono.just(current));
        when(rewardBatchRepository.findByMerchantIdAndPosType(MERCHANT_ID, PHYSICAL)).thenReturn(Flux.just(previousSent));
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.execute(MERCHANT_ID, BATCH_ID)).verifyComplete();
        verify(rewardBatchRepository).save(argThat(b -> b.getStatus() == RewardBatchStatus.SENT && b.getMerchantSendDate() != null));
    }
}

