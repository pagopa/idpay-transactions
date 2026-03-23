package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.common.web.dto.ErrorDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.ErogazioneOutcomeDTO;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
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

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CheckRewardBatchesOutcomesUseCaseTest {

    @Mock private RewardBatchRepository rewardBatchRepository;
    @Mock private ErogazioniRestClient erogazioniRestClient;
    private CheckRewardBatchesOutcomesUseCase useCase;
    private static final String BATCH_ID = "BATCH_ID";
    private static final String BATCH_ID_2 = "BATCH_ID_2";
    private static final String INITIATIVE_ID = "INITIATIVE_ID";

    @BeforeEach
    void setup() { useCase = new CheckRewardBatchesOutcomesUseCase(rewardBatchRepository, erogazioniRestClient); }

    @Test
    void execute_withIds_success() {
        RewardBatch batch1 = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        RewardBatch batch2 = RewardBatch.builder().id(BATCH_ID_2).status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO outcome1 = InvitaliaOutcomeResponseDTO.builder().message(null).erogazione(ErogazioneOutcomeDTO.builder().status("COMPLETATO").dateValue(LocalDate.now()).build()).build();
        InvitaliaOutcomeResponseDTO outcome2 = InvitaliaOutcomeResponseDTO.builder().erogazione(ErogazioneOutcomeDTO.builder().status("RIFIUTATO").build()).errors(List.of(new ErrorDTO("ERR01", "Errore"))).build();
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID, RewardBatchStatus.PENDING_REFUND)).thenReturn(Mono.just(batch1));
        when(rewardBatchRepository.findByIdAndStatus(BATCH_ID_2, RewardBatchStatus.PENDING_REFUND)).thenReturn(Mono.just(batch2));
        when(erogazioniRestClient.getOutcome(BATCH_ID)).thenReturn(Mono.just(outcome1));
        when(erogazioniRestClient.getOutcome(BATCH_ID_2)).thenReturn(Mono.just(outcome2));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.execute(INITIATIVE_ID, List.of(BATCH_ID, BATCH_ID_2))).verifyComplete();
        assertEquals(RewardBatchStatus.REFUNDED, batch1.getStatus());
        assertNotNull(batch1.getRefundValutaDate());
        assertEquals(RewardBatchStatus.NOT_REFUNDED, batch2.getStatus());
        assertEquals("ERR01 - Errore", batch2.getRefundErrorMessage());
    }

    @Test
    void execute_emptyList_success() {
        RewardBatch batch1 = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().message(null).erogazione(ErogazioneOutcomeDTO.builder().status("COMPLETATO").dateValue(LocalDate.now()).build()).build();
        when(rewardBatchRepository.findByStatus(RewardBatchStatus.PENDING_REFUND)).thenReturn(Flux.just(batch1));
        when(erogazioniRestClient.getOutcome(BATCH_ID)).thenReturn(Mono.just(outcome));
        when(rewardBatchRepository.save(any(RewardBatch.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.execute(INITIATIVE_ID, null)).verifyComplete();
        assertEquals(RewardBatchStatus.REFUNDED, batch1.getStatus());
    }

    @Test
    void updateBatch_completato_setsRefunded() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().message(null).erogazione(ErogazioneOutcomeDTO.builder().status("COMPLETATO").dateValue(LocalDate.now()).build()).build();
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.updateBatch(batch, outcome)).assertNext(b -> { assertEquals(RewardBatchStatus.REFUNDED, b.getStatus()); assertNotNull(b.getRefundValutaDate()); }).verifyComplete();
    }

    @Test
    void updateBatch_rifiutato_setsNotRefunded_withoutErrors() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().erogazione(ErogazioneOutcomeDTO.builder().status("RIFIUTATO").build()).errors(null).build();
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.updateBatch(batch, outcome)).assertNext(b -> { assertEquals(RewardBatchStatus.NOT_REFUNDED, b.getStatus()); assertNull(b.getRefundErrorMessage()); }).verifyComplete();
    }

    @Test
    void updateBatch_inLavorazione_withCreatedStatus_keepsCurrent() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.CREATED).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().erogazione(ErogazioneOutcomeDTO.builder().status("IN_LAVORAZIONE").build()).build();
        StepVerifier.create(useCase.updateBatch(batch, outcome)).assertNext(b -> assertEquals(RewardBatchStatus.CREATED, b.getStatus())).verifyComplete();
    }

    @Test
    void updateBatch_errore_withCreatedStatus_keepsCurrent() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.CREATED).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().erogazione(ErogazioneOutcomeDTO.builder().status("ERRORE").build()).build();
        StepVerifier.create(useCase.updateBatch(batch, outcome)).assertNext(b -> assertEquals(RewardBatchStatus.CREATED, b.getStatus())).verifyComplete();
    }

    @Test
    void updateBatch_rifiutato_withEmptyErrors_setsNotRefunded() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().erogazione(ErogazioneOutcomeDTO.builder().status("RIFIUTATO").build()).errors(List.of()).build();
        when(rewardBatchRepository.save(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        StepVerifier.create(useCase.updateBatch(batch, outcome)).assertNext(b -> { assertEquals(RewardBatchStatus.NOT_REFUNDED, b.getStatus()); assertNull(b.getRefundErrorMessage()); }).verifyComplete();
    }

    @Test
    void updateBatch_errore_withPendingStatus_keepsPendingRefund() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().erogazione(ErogazioneOutcomeDTO.builder().status("ERRORE").build()).build();
        StepVerifier.create(useCase.updateBatch(batch, outcome)).assertNext(b -> assertEquals(RewardBatchStatus.PENDING_REFUND, b.getStatus())).verifyComplete();
    }

    @Test
    void updateBatch_inLavorazione_withPendingStatus_keepsPendingRefund() {
        RewardBatch batch = RewardBatch.builder().id(BATCH_ID).status(RewardBatchStatus.PENDING_REFUND).build();
        InvitaliaOutcomeResponseDTO outcome = InvitaliaOutcomeResponseDTO.builder().erogazione(ErogazioneOutcomeDTO.builder().status("IN_LAVORAZIONE").build()).build();
        StepVerifier.create(useCase.updateBatch(batch, outcome)).assertNext(b -> assertEquals(RewardBatchStatus.PENDING_REFUND, b.getStatus())).verifyComplete();
    }
}

