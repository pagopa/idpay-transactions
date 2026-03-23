package it.gov.pagopa.idpay.transactions.usecase.rewardbatch;

import it.gov.pagopa.idpay.transactions.connector.rest.erogazioni.ErogazioniRestClient;
import it.gov.pagopa.idpay.transactions.connector.rest.invitalia.dto.InvitaliaOutcomeResponseDTO;
import it.gov.pagopa.idpay.transactions.enums.InvitaliaOutcomeStatus;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@AllArgsConstructor
public class CheckRewardBatchesOutcomesUseCase {

    private final RewardBatchRepository rewardBatchRepository;
    private final ErogazioniRestClient erogazioniRestClient;

    public Mono<Void> execute(String initiativeId, List<String> rewardBatchIds) {

        List<String> batchIds = rewardBatchIds != null ? rewardBatchIds : List.of();

        Flux<RewardBatch> batches;

        if (batchIds.isEmpty()) {
            batches = rewardBatchRepository.findByStatus(RewardBatchStatus.PENDING_REFUND);
        } else {
            batches = Flux.fromIterable(batchIds)
                    .flatMap(batchId ->
                            rewardBatchRepository.findByIdAndStatus(batchId, RewardBatchStatus.PENDING_REFUND)
                    );
        }

        return batches
                .flatMap(batch ->
                        erogazioniRestClient.getOutcome(batch.getId())
                                .flatMap(outcome -> updateBatch(batch, outcome))
                )
                .then();
    }

    public Mono<RewardBatch> updateBatch(RewardBatch batch, InvitaliaOutcomeResponseDTO response) {

        batch.setRefundOutcomeTimestamp(LocalDateTime.now());

        String status = response.getErogazione().getStatus();

        if (InvitaliaOutcomeStatus.COMPLETATO.name().equalsIgnoreCase(status)) {
            batch.setStatus(RewardBatchStatus.REFUNDED);
            batch.setRefundValutaDate(response.getErogazione().getDateValue());

        } else if (InvitaliaOutcomeStatus.RIFIUTATO.name().equalsIgnoreCase(status)) {

            batch.setStatus(RewardBatchStatus.NOT_REFUNDED);

            if (response.getErrors() != null && !response.getErrors().isEmpty()) {
                String errorMessage = response.getErrors().stream()
                        .map(error -> error.getCode() + " - " + error.getMessage())
                        .reduce((a, b) -> a + "; " + b)
                        .orElse(null);

                batch.setRefundErrorMessage(errorMessage);
            }

        } else if (InvitaliaOutcomeStatus.IN_LAVORAZIONE.name().equalsIgnoreCase(status) || InvitaliaOutcomeStatus.ERRORE.name().equalsIgnoreCase(status)) {
            log.info("Batch {} has not been processed with status {}, the external status is {}", batch.getId(), batch.getStatus(), status);
            return Mono.just(batch);
        }

        logOutcomeTransition(batch);

        return rewardBatchRepository.save(batch);
    }

    private void logOutcomeTransition(RewardBatch batch) {
        log.info("Batch {} outcome processed, setting status {}", batch.getId(), batch.getStatus());
    }
}

