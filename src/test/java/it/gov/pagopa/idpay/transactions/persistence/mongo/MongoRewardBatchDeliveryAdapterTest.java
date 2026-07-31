package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.LocalDate;
import java.time.Month;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class MongoRewardBatchDeliveryAdapterTest {

    private static final String BATCH_ID = "batch";
    private static final String INITIATIVE_ID = "initiative";

    @Mock
    private RewardBatchRepository rewardBatchRepository;

    @Test
    void snapshotDeliveryAmount_savesPositiveApprovedAmountOnlyOnce() {
        RewardBatch withoutSnapshot = batch(RewardBatchStatus.APPROVED, 250L, null);
        RewardBatch withSnapshot = batch(RewardBatchStatus.APPROVED, 500L, 250L);

        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.APPROVED
        )).thenReturn(Mono.just(withoutSnapshot), Mono.just(withSnapshot));
        when(rewardBatchRepository.save(withoutSnapshot)).thenReturn(Mono.just(withoutSnapshot));

        StepVerifier.create(adapter().snapshotDeliveryAmount(BATCH_ID, INITIATIVE_ID))
                .assertNext(result -> {
                    assertEquals(250L, result.getDeliveryAmountCents());
                    assertNotNull(result.getUpdateDate());
                })
                .verifyComplete();
        StepVerifier.create(adapter().snapshotDeliveryAmount(BATCH_ID, INITIATIVE_ID))
                .expectNext(withSnapshot)
                .verifyComplete();

        verify(rewardBatchRepository).save(withoutSnapshot);
        verify(rewardBatchRepository, never()).save(same(withSnapshot));
    }

    @Test
    void snapshotDeliveryAmount_ignoresNonPositiveOrMissingAmount() {
        RewardBatch zeroAmount = batch(RewardBatchStatus.APPROVED, 0L, null);
        RewardBatch missingAmount = batch(RewardBatchStatus.APPROVED, null, null);
        when(rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                BATCH_ID, INITIATIVE_ID, RewardBatchStatus.APPROVED
        )).thenReturn(Mono.just(zeroAmount), Mono.just(missingAmount));

        StepVerifier.create(adapter().snapshotDeliveryAmount(BATCH_ID, INITIATIVE_ID)).verifyComplete();
        StepVerifier.create(adapter().snapshotDeliveryAmount(BATCH_ID, INITIATIVE_ID)).verifyComplete();

        verify(rewardBatchRepository, never()).save(same(zeroAmount));
        verify(rewardBatchRepository, never()).save(same(missingAmount));
    }

    @Test
    void recordDeliveryOutcome_rejectsNullOutcomeAndApprovedBatchWithoutSnapshot() {
        StepVerifier.create(adapter().recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, null))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "Delivery outcome is required".equals(error.getMessage()))
                .verify();

        RewardBatch withoutSnapshot = batch(RewardBatchStatus.APPROVED, 250L, null);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(withoutSnapshot));

        StepVerifier.create(adapter().recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, outcome(true)))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("not snapshotted"))
                .verify();
        verify(rewardBatchRepository, never()).save(withoutSnapshot);
    }

    @Test
    void recordDeliveryOutcome_movesAcceptedDeliveryToPendingRefundAndIsIdempotentOnRetry() {
        RewardBatch approved = batch(RewardBatchStatus.APPROVED, 250L, 250L);
        RewardBatch pendingRefund = batch(RewardBatchStatus.PENDING_REFUND, 250L, 250L);
        DeliveryOutcomeDTO accepted = outcome(true);

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(approved), Mono.just(pendingRefund));
        when(rewardBatchRepository.save(approved)).thenReturn(Mono.just(approved));

        StepVerifier.create(adapter().recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, accepted))
                .assertNext(result -> {
                    assertEquals(RewardBatchStatus.PENDING_REFUND, result.getStatus());
                    assertEquals(accepted, result.getDeliveryOutcome());
                    assertNotNull(result.getDeliveryDateRequest());
                })
                .verifyComplete();
        StepVerifier.create(adapter().recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, accepted))
                .expectNext(pendingRefund)
                .verifyComplete();

        verify(rewardBatchRepository).save(approved);
        verify(rewardBatchRepository, never()).save(same(pendingRefund));
    }

    @Test
    void recordDeliveryOutcome_keepsApprovedStatusWhenDeliveryIsRejected() {
        RewardBatch approved = batch(RewardBatchStatus.APPROVED, 250L, 250L);
        DeliveryOutcomeDTO rejected = outcome(false);

        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(approved));
        when(rewardBatchRepository.save(approved)).thenReturn(Mono.just(approved));

        StepVerifier.create(adapter().recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, rejected))
                .assertNext(result -> {
                    assertEquals(RewardBatchStatus.APPROVED, result.getStatus());
                    assertEquals(rejected, result.getDeliveryOutcome());
                    assertNotNull(result.getUpdateDate());
                })
                .verifyComplete();

        verify(rewardBatchRepository).save(approved);
    }

    @Test
    void recordDeliveryOutcome_ignoresNonApprovedBatchWithoutSaving() {
        RewardBatch sent = batch(RewardBatchStatus.SENT, 250L, 250L);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(sent));

        StepVerifier.create(adapter().recordDeliveryOutcome(BATCH_ID, INITIATIVE_ID, outcome(false)))
                .verifyComplete();

        verify(rewardBatchRepository, never()).save(sent);
    }

    @Test
    void recordRefundOutcome_rejectsUnsupportedStatusWithoutSaving() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> adapter().recordRefundOutcome(
                        BATCH_ID, INITIATIVE_ID, RewardBatchStatus.APPROVED, null, null
                )
        );

        assertEquals("Unsupported refund outcome status", error.getMessage());
        verify(rewardBatchRepository, never()).save(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void recordRefundOutcome_movesPendingRefundToRefundedAndPersistsTerminalDetails() {
        RewardBatch pendingRefund = batch(RewardBatchStatus.PENDING_REFUND, 250L, 250L);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(pendingRefund));
        when(rewardBatchRepository.save(pendingRefund)).thenReturn(Mono.just(pendingRefund));

        StepVerifier.create(adapter().recordRefundOutcome(
                        BATCH_ID, INITIATIVE_ID, RewardBatchStatus.REFUNDED, LocalDate.of(2026, Month.JULY, 10), null
                ))
                .assertNext(result -> {
                    assertEquals(RewardBatchStatus.REFUNDED, result.getStatus());
                    assertEquals(LocalDate.of(2026, Month.JULY, 10), result.getRefundValutaDate());
                    assertNotNull(result.getRefundOutcomeTimestamp());
                })
                .verifyComplete();

        verify(rewardBatchRepository).save(pendingRefund);
    }

    @Test
    void recordRefundOutcome_isIdempotentForTerminalStateAndIgnoresOtherLifecycleStates() {
        RewardBatch refunded = batch(RewardBatchStatus.REFUNDED, 250L, 250L);
        RewardBatch approved = batch(RewardBatchStatus.APPROVED, 250L, 250L);
        when(rewardBatchRepository.findRewardBatchByIdAndInitiativeId(BATCH_ID, INITIATIVE_ID))
                .thenReturn(Mono.just(refunded), Mono.just(approved));

        StepVerifier.create(adapter().recordRefundOutcome(
                        BATCH_ID, INITIATIVE_ID, RewardBatchStatus.REFUNDED, null, null
                ))
                .expectNext(refunded)
                .verifyComplete();
        StepVerifier.create(adapter().recordRefundOutcome(
                        BATCH_ID, INITIATIVE_ID, RewardBatchStatus.REFUNDED, null, null
                ))
                .verifyComplete();

        verify(rewardBatchRepository, never()).save(same(refunded));
        verify(rewardBatchRepository, never()).save(same(approved));
    }

    private MongoRewardBatchDeliveryAdapter adapter() {
        return new MongoRewardBatchDeliveryAdapter(rewardBatchRepository);
    }

    private static RewardBatch batch(RewardBatchStatus status, Long approvedAmount, Long deliveryAmount) {
        return RewardBatch.builder()
                .id(BATCH_ID)
                .initiativeId(INITIATIVE_ID)
                .status(status)
                .approvedAmountCents(approvedAmount)
                .deliveryAmountCents(deliveryAmount)
                .build();
    }

    private static DeliveryOutcomeDTO outcome(boolean succeeded) {
        return DeliveryOutcomeDTO.builder().succeded(succeeded).message("outcome").code(200).build();
    }
}
