package it.gov.pagopa.idpay.transactions.persistence.port;

import it.gov.pagopa.idpay.transactions.dto.DeliveryOutcomeDTO;
import it.gov.pagopa.idpay.transactions.enums.RewardBatchStatus;
import it.gov.pagopa.idpay.transactions.model.RewardBatch;
import java.time.LocalDate;

import it.gov.pagopa.idpay.transactions.persistence.sql.OutcomeCheckCommand;
import org.jooq.JSONB;
import reactor.core.publisher.Mono;

public interface RewardBatchDeliveryPort {

    Mono<RewardBatch> snapshotDeliveryAmount(String rewardBatchId, String initiativeId);

    Mono<RewardBatch> recordDeliveryOutcome(
            String rewardBatchId,
            String initiativeId,
            DeliveryOutcomeDTO deliveryOutcome
    );

    Mono<Void> recordDeliveryEvent(
            String rewardBatchId,
            String initiativeId,
            String outcome,
            Integer httpCode,
            JSONB requestJson,
            JSONB responseJson
    );

    Mono<Void> recordOutcomeEvent(
            String rewardBatchId,
            String initiativeId,
            String invitaliaStatus,
            Long amountPaidCents,
            JSONB responseJson
    );

    Mono<RewardBatch> recordRefundOutcome(
            String rewardBatchId,
            String initiativeId,
            RewardBatchStatus status,
            LocalDate refundValutaDate,
            String refundErrorMessage
    );

    Mono<RewardBatch> recordOutcomeCheck(
            String rewardBatchId,
            String initiativeId,
            OutcomeCheckCommand command,
            JSONB responseJson
    );
}
