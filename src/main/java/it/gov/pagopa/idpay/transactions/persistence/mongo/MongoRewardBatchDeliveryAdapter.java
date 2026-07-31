package it.gov.pagopa.idpay.transactions.persistence.mongo;

import static it.gov.pagopa.common.utils.CommonConstants.ZONEID;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import it.gov.pagopa.idpay.transactions.persistence.port.RewardBatchDeliveryPort;
import it.gov.pagopa.idpay.transactions.repository.RewardBatchRepository;
import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class MongoRewardBatchDeliveryAdapter implements RewardBatchDeliveryPort {

    private final RewardBatchRepository rewardBatchRepository;

    @Override
    public Mono<RewardBatch> snapshotDeliveryAmount(String rewardBatchId, String initiativeId) {
        return rewardBatchRepository.findByIdAndInitiativeIdAndStatus(
                        rewardBatchId,
                        initiativeId,
                        RewardBatchStatus.APPROVED
                )
                .filter(batch -> deliveryAmount(batch) > 0L)
                .flatMap(batch -> {
                    if (batch.getDeliveryAmountCents() != null) {
                        return Mono.just(batch);
                    }

                    batch.setDeliveryAmountCents(batch.getApprovedAmountCents());
                    batch.setUpdateDate(LocalDateTime.now(ZONEID));
                    return rewardBatchRepository.save(batch);
                });
    }

    @Override
    public Mono<RewardBatch> recordDeliveryOutcome(
            String rewardBatchId,
            String initiativeId,
            DeliveryOutcomeDTO deliveryOutcome
    ) {
        if (deliveryOutcome == null) {
            return Mono.error(new IllegalArgumentException("Delivery outcome is required"));
        }

        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(rewardBatchId, initiativeId)
                .flatMap(batch -> {
                    if (deliveryOutcome.isSucceded()
                            && batch.getStatus() == RewardBatchStatus.PENDING_REFUND) {
                        return Mono.just(batch);
                    }
                    if (batch.getStatus() != RewardBatchStatus.APPROVED) {
                        return Mono.empty();
                    }
                    if (batch.getDeliveryAmountCents() == null) {
                        return Mono.error(new IllegalStateException(
                                "Delivery amount was not snapshotted for batch %s".formatted(rewardBatchId)
                        ));
                    }

                    batch.setDeliveryOutcome(deliveryOutcome);
                    batch.setUpdateDate(LocalDateTime.now(ZONEID));
                    if (deliveryOutcome.isSucceded()) {
                        batch.setStatus(RewardBatchStatus.PENDING_REFUND);
                        batch.setDeliveryDateRequest(LocalDateTime.now(ZONEID));
                    }
                    return rewardBatchRepository.save(batch);
                });
    }

    @Override
    public Mono<RewardBatch> recordRefundOutcome(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status,
            LocalDate refundValutaDate,
            String refundErrorMessage
    ) {
        validateRefundStatus(status);
        return rewardBatchRepository.findRewardBatchByIdAndInitiativeId(rewardBatchId, initiativeId)
                .flatMap(batch -> {
                    if (batch.getStatus() == status) {
                        return Mono.just(batch);
                    }
                    if (batch.getStatus() != RewardBatchStatus.PENDING_REFUND) {
                        return Mono.empty();
                    }

                    batch.setStatus(status);
                    batch.setRefundValutaDate(refundValutaDate);
                    batch.setRefundErrorMessage(refundErrorMessage);
                    batch.setRefundOutcomeTimestamp(LocalDateTime.now(ZONEID));
                    batch.setUpdateDate(LocalDateTime.now(ZONEID));
                    return rewardBatchRepository.save(batch);
                });
    }

    private static long deliveryAmount(RewardBatch batch) {
        Long amount = batch.getDeliveryAmountCents() == null
                ? batch.getApprovedAmountCents()
                : batch.getDeliveryAmountCents();
        return amount == null ? 0L : amount;
    }

    private static void validateRefundStatus(RewardBatchStatus status) {
        if (status != RewardBatchStatus.REFUNDED && status != RewardBatchStatus.NOT_REFUNDED) {
            throw new IllegalArgumentException("Unsupported refund outcome status");
        }
    }
}
