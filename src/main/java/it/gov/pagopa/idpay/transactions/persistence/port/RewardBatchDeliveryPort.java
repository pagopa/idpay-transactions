package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDate;
import reactor.core.publisher.Mono;

public interface RewardBatchDeliveryPort {

    Mono<RewardBatch> snapshotDeliveryAmount(String rewardBatchId, String initiativeId);

    Mono<RewardBatch> recordDeliveryOutcome(
            String rewardBatchId,
            String initiativeId,
            DeliveryOutcomeDTO deliveryOutcome
    );

    Mono<RewardBatch> recordRefundOutcome(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status,
            LocalDate refundValutaDate,
            String refundErrorMessage
    );
}
